#!/usr/bin/env python3
"""
MQTT ↔ RS-485 agent for MDV/Midea fancoil (XYE protocol).

Reads protocol schema from schema.yaml, communicates with the fancoil
over RS-485, and bridges status/commands via MQTT.

Includes an interactive test mode (--test) for protocol verification
without MQTT.

Usage:
    python mqtt_rs485_agent.py --config schema.yaml --debug          # MQTT mode
    python mqtt_rs485_agent.py --config schema.yaml --test --debug   # Interactive test mode
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
from typing import Any

import yaml
import serial
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger("xye-agent")


def setup_logging(level_name: str = "INFO") -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def hex_dump(data: bytes | bytearray) -> str:
    """Format bytes as hex string: 'FE AA C0 ...'"""
    return " ".join(f"{b:02X}" for b in data)


def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base*."""
    result = base.copy()
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def load_config(path: str) -> dict:
    """Load schema.yaml and merge .env.yaml if present."""
    with open(path, "r", encoding="utf-8") as fp:
        cfg = yaml.safe_load(fp)

    env_path = os.path.join(os.path.dirname(path) or ".", ".env.yaml")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as fp:
            env = yaml.safe_load(fp) or {}
        cfg = deep_merge(cfg, env)

    return cfg


# ---------------------------------------------------------------------------
# XYE Protocol Layer
# ---------------------------------------------------------------------------

class XYEProtocol:
    """
    Builds TX frames, parses RX frames, calculates / validates CRC.

    All byte offsets come from the schema — the code has **zero** hard-coded
    magic numbers.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.tx_cfg = cfg["tx"]
        self.rx_cfg = cfg["rx"]
        self.crc_cfg = cfg["crc"]
        self.fields = cfg["fields"]

        self.tx_offsets = self.tx_cfg["offsets"]
        self.rx_offsets = self.rx_cfg["offsets"]

        self.tx_len = int(self.tx_cfg["frame_length"])
        self.rx_len = int(self.rx_cfg["frame_length"])

        self.crc_start = int(self.crc_cfg["start_offset"])
        self.crc_addend = int(self.crc_cfg["addend"])

    # ---- CRC ---------------------------------------------------------------

    def calc_crc(self, frame: bytearray, crc_pos: int) -> int:
        """
        CRC = 255 - (sum(frame[crc_start .. crc_pos-1]) + addend) % 256
        """
        s = sum(frame[self.crc_start:crc_pos]) + self.crc_addend
        return 255 - (s % 256)

    def verify_crc(self, frame: bytes, crc_pos: int) -> bool:
        expected = self.calc_crc(bytearray(frame), crc_pos)
        actual = frame[crc_pos]
        if expected != actual:
            log.warning(
                "CRC mismatch: calculated 0x%02X, got 0x%02X", expected, actual
            )
        return expected == actual

    # ---- TX frame builders -------------------------------------------------

    def _finalize_tx(self, frame: bytearray, address: int, master_id: int) -> bytearray:
        """Fill destination address, source id, compute CRC."""
        frame[self.tx_offsets["destination"]] = address & 0x3F
        frame[self.tx_offsets["source_id"]] = master_id & 0x3F
        crc_pos = self.tx_offsets["crc"]
        frame[crc_pos] = self.calc_crc(frame, crc_pos)
        return frame

    def build_query(self, address: int, master_id: int = 0) -> bytearray:
        frame = bytearray(self.tx_cfg["query"]["template"])
        return self._finalize_tx(frame, address, master_id)

    def build_set(
        self,
        address: int,
        *,
        power: bool = True,
        mode: str = "FAN",
        fan: str = "AUTO",
        temp: int = 24,
        mode_flags: list[str] | None = None,
        timer_start: int = 0,
        timer_stop: int = 0,
        master_id: int = 0,
    ) -> bytearray:
        frame = bytearray(self.tx_cfg["set"]["template"])
        pay = self.tx_cfg["set"]["payload"]

        # Mode byte: mode_value | 0x80 if power on, else 0x00
        mode_upper = mode.upper()
        mode_val = self.fields["modes"].get(mode_upper)
        if mode_val is None:
            raise ValueError(f"Unknown mode '{mode}'; valid: {list(self.fields['modes'])}")
        frame[pay["mode"]] = (mode_val | 0x80) if power else 0x00

        # Fan byte
        fan_upper = fan.upper()
        fan_val = self.fields["fan"].get(fan_upper)
        if fan_val is None:
            raise ValueError(f"Unknown fan speed '{fan}'; valid: {list(self.fields['fan'])}")
        frame[pay["fan"]] = fan_val

        # Temperature
        if mode_upper == "FAN":
            frame[pay["temp"]] = 0xFF
        else:
            limits = self.rx_cfg.get("temp_limits", {})
            t_min = limits.get("min", 17)
            t_max = limits.get("max", 30)
            clamped = max(t_min, min(t_max, temp))
            if clamped != temp:
                log.warning("Temperature %d clamped to %d (range %d–%d)", temp, clamped, t_min, t_max)
            frame[pay["temp"]] = clamped

        # Mode flags (bitmask)
        flags_byte = 0
        for flag_name in (mode_flags or []):
            fv = self.fields["mode_flags"].get(flag_name.upper())
            if fv is None:
                raise ValueError(
                    f"Unknown mode flag '{flag_name}'; valid: {list(self.fields['mode_flags'])}"
                )
            flags_byte |= fv
        frame[pay["mode_flags"]] = flags_byte

        # Timers
        frame[pay["timer_start"]] = timer_start & 0xFF
        frame[pay["timer_stop"]] = timer_stop & 0xFF

        return self._finalize_tx(frame, address, master_id)

    def build_lock(self, address: int, master_id: int = 0) -> bytearray:
        frame = bytearray(self.tx_cfg["lock"]["template"])
        return self._finalize_tx(frame, address, master_id)

    def build_unlock(self, address: int, master_id: int = 0) -> bytearray:
        frame = bytearray(self.tx_cfg["unlock"]["template"])
        return self._finalize_tx(frame, address, master_id)

    # ---- RX frame parser ---------------------------------------------------

    def find_rx_frame(self, buf: bytes) -> bytes | None:
        """
        Locate a valid RX frame inside a raw byte buffer.

        Scans for FE AA, then checks length and CRC.
        """
        target_len = self.rx_len
        for i in range(len(buf) - target_len + 1):
            if buf[i] == 0xFE and buf[i + 1] == 0xAA:
                candidate = buf[i : i + target_len]
                crc_pos = self.rx_offsets["crc"]
                if self.verify_crc(candidate, crc_pos):
                    return bytes(candidate)
                else:
                    log.debug(
                        "Frame at offset %d has bad CRC, skipping", i
                    )
        return None

    def parse_rx(self, frame: bytes) -> dict[str, Any] | None:
        """Parse a validated RX frame into a state dict."""
        if len(frame) < self.rx_len:
            log.warning("Frame too short: %d < %d", len(frame), self.rx_len)
            return None

        off = self.rx_offsets

        # Raw field extraction
        response_code = frame[off["response_code"]]
        source_addr = frame[off["source"]]
        capabilities = frame[off["capabilities"]]
        mode_raw = frame[off["mode"]]
        fan_raw = frame[off["fan"]]
        setpoint = frame[off["setpoint"]]
        t1_raw = frame[off["t1"]]
        t2a_raw = frame[off["t2a"]]
        t2b_raw = frame[off["t2b"]]
        t3_raw = frame[off["t3"]]
        current_raw = frame[off["current"]]
        timer_start = frame[off["timer_start"]]
        unknown_13 = frame[off["unknown_13"]]
        unknown_14 = frame[off.get("unknown_14", 21)] if "unknown_14" in off else 0
        mode_flags_raw = frame[off["mode_flags"]]
        oper_flags_raw = frame[off["oper_flags"]]
        error_h = frame[off["error_high"]]
        error_l = frame[off["error_low"]]
        protect_h = frame[off["protect_high"]]
        protect_l = frame[off["protect_low"]]
        ccm_err = frame[off["ccm_error"]]

        # Decode mode
        power = bool(mode_raw & 0x80)
        mode_bits = mode_raw & 0x1F
        mode_name = "UNKNOWN"
        for name, val in self.fields["modes"].items():
            if val != 0 and val == mode_bits:
                mode_name = name
                break
        if mode_raw == 0x00:
            mode_name = "OFF"

        # Decode fan — composite byte: bit7=AUTO, bit2=status, bits0-1=speed
        fan_auto = bool(fan_raw & 0x80)
        fan_status_bit = bool(fan_raw & 0x04)
        fan_speed_bits = fan_raw & 0x03
        if fan_auto:
            fan_name = "AUTO"
        elif fan_speed_bits == 0:
            fan_name = "MANUAL"  # MKG-300C doesn't report which manual speed
        else:
            # Try to match speed value from schema
            fan_name = "UNKNOWN"
            for name, val in self.fields["fan"].items():
                if val == fan_speed_bits:
                    fan_name = name
                    break

        # Decode mode flags
        active_flags = []
        for name, mask in self.fields["mode_flags"].items():
            if mode_flags_raw & mask == mask:
                active_flags.append(name)

        # Decode oper flags
        active_oper = []
        for name, mask in self.fields["oper_flags"].items():
            if oper_flags_raw & mask == mask:
                active_oper.append(name)

        # Temperature sensors
        sensor_cfg = self.rx_cfg.get("temp_sensor", {})
        invalid_val = sensor_cfg.get("invalid", 0xFF)

        def decode_temp_sensor(raw: int) -> float | None:
            if raw == invalid_val:
                return None
            return (raw - sensor_cfg.get("offset", 48)) * sensor_cfg.get("scale", 0.5)

        # Error / protection codes
        errors = self._decode_error_bits(error_h, error_l, "E")
        protections = self._decode_error_bits(protect_h, protect_l, "P")

        state = {
            "address": source_addr,
            "response_code": f"0x{response_code:02X}",
            "power": power,
            "mode": mode_name,
            "mode_raw": f"0x{mode_raw:02X}",
            "fan": fan_name,
            "fan_auto": fan_auto,
            "fan_status_bit": fan_status_bit,
            "fan_speed_bits": fan_speed_bits,
            "fan_raw": f"0x{fan_raw:02X}",
            "setpoint": setpoint,
            "capabilities": f"0x{capabilities:02X}",
            "t1": decode_temp_sensor(t1_raw),
            "t2a": decode_temp_sensor(t2a_raw),
            "t2b": decode_temp_sensor(t2b_raw),
            "t3": decode_temp_sensor(t3_raw),
            "t1_raw": f"0x{t1_raw:02X}",
            "t2a_raw": f"0x{t2a_raw:02X}",
            "t2b_raw": f"0x{t2b_raw:02X}",
            "t3_raw": f"0x{t3_raw:02X}",
            "current": current_raw if current_raw != 0xFF else None,
            "current_raw": f"0x{current_raw:02X}",
            "timer_start": timer_start,
            "mode_flags": active_flags,
            "mode_flags_raw": f"0x{mode_flags_raw:02X}",
            "oper_flags": active_oper,
            "oper_flags_raw": f"0x{oper_flags_raw:02X}",
            "errors": errors,
            "protections": protections,
            "ccm_error": ccm_err,
            "unknown_13": f"0x{unknown_13:02X}",
            "unknown_14": f"0x{unknown_14:02X}",
            "raw": hex_dump(frame),
        }
        return state

    @staticmethod
    def _decode_error_bits(high: int, low: int, prefix: str) -> list[str]:
        codes = []
        for bit in range(8):
            if high & (1 << bit):
                codes.append(f"{prefix}{bit}")
        for bit in range(8):
            if low & (1 << bit):
                codes.append(f"{prefix}{8 + bit}")
        return codes


# ---------------------------------------------------------------------------
# Serial Transport
# ---------------------------------------------------------------------------

class SerialTransport:
    """Manages RS-485 serial port: open/close/reconnect, send, receive."""

    def __init__(self, cfg: dict):
        self.cfg = cfg["serial"]
        self.port_name = self.cfg["port"]
        self.read_timeout = float(self.cfg.get("read_timeout", 0.5))
        self._port: serial.Serial | None = None

    @property
    def is_open(self) -> bool:
        return self._port is not None and self._port.is_open

    def open(self) -> None:
        if self.is_open:
            return
        parity_map = {"N": serial.PARITY_NONE, "E": serial.PARITY_EVEN,
                       "O": serial.PARITY_ODD}
        self._port = serial.Serial(
            port=self.port_name,
            baudrate=int(self.cfg.get("baud", 4800)),
            bytesize=int(self.cfg.get("bytesize", 8)),
            parity=parity_map.get(self.cfg.get("parity", "N"), serial.PARITY_NONE),
            stopbits=int(self.cfg.get("stopbits", 1)),
            timeout=0.05,  # short timeout for non-blocking reads
        )
        log.info("Serial port %s opened", self.port_name)

    def close(self) -> None:
        if self._port and self._port.is_open:
            self._port.close()
            log.info("Serial port %s closed", self.port_name)
        self._port = None

    def reconnect(self) -> bool:
        self.close()
        try:
            self.open()
            return True
        except serial.SerialException as exc:
            log.error("Serial reconnect failed: %s", exc)
            return False

    def send(self, data: bytearray) -> bool:
        if not self.is_open:
            log.error("Cannot send: port closed")
            return False
        assert self._port is not None
        try:
            self._port.write(data)
            self._port.flush()
            log.debug("TX (%d): %s", len(data), hex_dump(data))
            return True
        except serial.SerialException as exc:
            log.error("Serial write error: %s", exc)
            return False

    def receive(self, expected_len: int) -> bytes:
        """
        Read bytes until we get *expected_len* or timeout.
        Returns whatever was collected.
        """
        if not self.is_open:
            return b""
        assert self._port is not None
        buf = bytearray()
        deadline = time.time() + self.read_timeout
        while time.time() < deadline:
            remaining = expected_len - len(buf)
            if remaining <= 0:
                break
            try:
                chunk = self._port.read(remaining)
            except serial.SerialException as exc:
                log.error("Serial read error: %s", exc)
                break
            if chunk:
                buf.extend(chunk)
            else:
                time.sleep(0.01)
        if buf:
            log.debug("RX (%d): %s", len(buf), hex_dump(buf))
        else:
            log.debug("RX: no data")
        return bytes(buf)

    def drain(self) -> None:
        """Discard any bytes sitting in the input buffer."""
        if self.is_open and self._port is not None:
            self._port.reset_input_buffer()


# ---------------------------------------------------------------------------
# MQTT Bridge
# ---------------------------------------------------------------------------

class MQTTBridge:
    """Thin MQTT wrapper: connect, publish state, receive commands."""

    def __init__(self, cfg: dict, command_callback):
        self.cfg = cfg["mqtt"]
        self.topics = self.cfg["topics"]
        self.qos = int(self.cfg.get("qos", 0))
        self._command_cb = command_callback
        self._client = mqtt.Client(
            client_id=self.cfg.get("client_id", "mdv-agent")
        )
        if self.cfg.get("username"):
            self._client.username_pw_set(
                self.cfg["username"], self.cfg.get("password", "")
            )
        self._client.will_set(
            self.topics["availability"], "offline", qos=self.qos, retain=True
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def start(self) -> None:
        broker = self.cfg.get("broker", "localhost")
        port = int(self.cfg.get("port", 1883))
        log.info("Connecting to MQTT broker %s:%d", broker, port)
        self._client.connect(broker, port)
        self._client.loop_start()

    def stop(self) -> None:
        self.publish_availability("offline")
        self._client.loop_stop()
        self._client.disconnect()

    def publish_availability(self, value: str) -> None:
        self._client.publish(
            self.topics["availability"], value, qos=self.qos, retain=True
        )

    def publish_state(self, state: dict) -> None:
        payload = json.dumps(state, ensure_ascii=False)
        self._client.publish(
            self.topics["state"], payload, qos=self.qos, retain=True
        )
        log.debug("Published state to %s", self.topics["state"])

    def _on_connect(self, client, userdata, flags, rc):
        log.info("MQTT connected (rc=%d)", rc)
        client.subscribe(self.topics["set"])
        self.publish_availability("online")

    def _on_disconnect(self, client, userdata, rc):
        log.warning("MQTT disconnected (rc=%d)", rc)

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            log.info("MQTT command received: %s", payload)
            data = json.loads(payload)
            self._command_cb(data)
        except Exception:
            log.exception("Failed to decode MQTT command")


# ---------------------------------------------------------------------------
# Agent — main loop (MQTT mode)
# ---------------------------------------------------------------------------

class Agent:
    """
    Main agent: polls the fancoil, publishes state, accepts commands.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.protocol = XYEProtocol(cfg)
        self.transport = SerialTransport(cfg)
        self.command_queue: queue.Queue[dict] = queue.Queue()
        self.mqtt = MQTTBridge(cfg, self.command_queue.put)
        self.address = int(cfg["serial"].get("address", 0))
        self.master_id = int(cfg["serial"].get("address", 0))
        self.poll_interval = float(cfg["serial"].get("poll_interval", 1.0))
        self.inter_cmd_delay = float(cfg["serial"].get("inter_command_delay", 0.1))
        self.last_state: dict | None = None
        self._stop = threading.Event()

    def _transact(self, frame: bytearray) -> dict | None:
        """Send frame, read response, parse it. Returns state dict or None."""
        self.transport.drain()
        if not self.transport.send(frame):
            return None
        raw = self.transport.receive(self.protocol.rx_len + 4)  # small margin
        if not raw:
            return None
        rx = self.protocol.find_rx_frame(raw)
        if rx is None:
            log.warning("No valid RX frame found in %d bytes", len(raw))
            return None
        state = self.protocol.parse_rx(rx)
        return state

    def poll(self) -> dict | None:
        frame = self.protocol.build_query(self.address, self.master_id)
        return self._transact(frame)

    def send_command(self, cmd: dict) -> dict | None:
        frame = self.protocol.build_set(
            self.address,
            power=bool(cmd.get("power", True)),
            mode=cmd.get("mode", "FAN"),
            fan=cmd.get("fan", cmd.get("speed", "AUTO")),
            temp=int(cmd.get("temp", 24)),
            mode_flags=cmd.get("mode_flags", []),
            timer_start=int(cmd.get("timer_start", 0)),
            timer_stop=int(cmd.get("timer_stop", 0)),
            master_id=self.master_id,
        )
        return self._transact(frame)

    def run(self) -> None:
        self.transport.open()
        self.mqtt.start()
        next_poll = time.time()
        try:
            while not self._stop.is_set():
                # Process commands
                try:
                    cmd = self.command_queue.get(block=False)
                    state = self.send_command(cmd)
                    if state:
                        self.last_state = state
                        self.mqtt.publish_state(state)
                    time.sleep(self.inter_cmd_delay)
                except queue.Empty:
                    pass

                # Periodic poll
                now = time.time()
                if now >= next_poll:
                    state = self.poll()
                    if state:
                        self.last_state = state
                        self.mqtt.publish_state(state)
                    else:
                        log.debug("Poll returned no data")
                    next_poll = now + self.poll_interval

                time.sleep(0.01)
        except KeyboardInterrupt:
            log.info("Interrupted")
        finally:
            self.mqtt.stop()
            self.transport.close()


# ---------------------------------------------------------------------------
# Interactive Test Mode
# ---------------------------------------------------------------------------

class InteractiveTester:
    """
    Interactive shell for protocol verification.

    Commands:
        query [addr]                     — Send GET and print parsed response
        set <mode> <fan> <temp> [addr]   — Send SET command
        lock [addr]                      — Lock unit
        unlock [addr]                    — Unlock unit
        raw <hex bytes>                  — Send raw bytes and read response
        crc <hex bytes>                  — Calculate CRC for given bytes
        scan [start] [end]               — Scan for devices on the bus
        help                             — Show commands
        quit / exit                      — Exit
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.protocol = XYEProtocol(cfg)
        self.transport = SerialTransport(cfg)
        self.address = int(cfg["serial"].get("address", 0))
        self.master_id = int(cfg["serial"].get("address", 0))

    def run(self) -> None:
        self.transport.open()
        print("\n=== XYE Protocol Interactive Tester ===")
        print(f"Port: {self.cfg['serial']['port']}, Address: 0x{self.address:02X}")
        print("Type 'help' for available commands.\n")

        try:
            while True:
                try:
                    line = input("xye> ").strip()
                except EOFError:
                    break
                if not line:
                    continue
                parts = line.split()
                cmd = parts[0].lower()
                args = parts[1:]
                try:
                    if cmd in ("quit", "exit", "q"):
                        break
                    elif cmd == "help":
                        self._help()
                    elif cmd in ("query", "get", "poll"):
                        self._cmd_query(args)
                    elif cmd == "set":
                        self._cmd_set(args)
                    elif cmd == "lock":
                        self._cmd_lock(args)
                    elif cmd == "unlock":
                        self._cmd_unlock(args)
                    elif cmd == "raw":
                        self._cmd_raw(args)
                    elif cmd == "crc":
                        self._cmd_crc(args)
                    elif cmd == "scan":
                        self._cmd_scan(args)
                    else:
                        print(f"Unknown command: {cmd}. Type 'help'.")
                except Exception as exc:
                    print(f"Error: {exc}")
        finally:
            self.transport.close()

    def _help(self):
        print("""
Commands:
  query [addr]                   — GET status (default addr from config)
  set <mode> <fan> <temp> [addr] — SET command (e.g. set COOL LOW 22)
  lock [addr]                    — Lock the unit
  unlock [addr]                  — Unlock the unit
  raw <hex bytes>                — Send raw hex (e.g. raw FE AA C0 00 ...)
  crc <hex bytes>                — Calculate CRC for given frame
  scan [start] [end]             — Scan bus addresses (default 0-63)
  help                           — This help
  quit / exit                    — Exit tester
""")

    def _transact(self, frame: bytearray) -> dict | None:
        print(f"  TX ({len(frame)}): {hex_dump(frame)}")
        self.transport.drain()
        if not self.transport.send(frame):
            print("  Send failed!")
            return None
        raw = self.transport.receive(self.protocol.rx_len + 4)
        if not raw:
            print("  No response.")
            return None
        print(f"  RX ({len(raw)}): {hex_dump(raw)}")
        rx = self.protocol.find_rx_frame(raw)
        if rx is None:
            print("  Could not find valid frame in response.")
            return None
        state = self.protocol.parse_rx(rx)
        if state:
            self._print_state(state)
        return state

    def _parse_addr(self, args: list[str], pos: int = 0) -> int:
        if len(args) > pos:
            return int(args[pos], 0)
        return self.address

    def _cmd_query(self, args: list[str]):
        addr = self._parse_addr(args, 0)
        frame = self.protocol.build_query(addr, self.master_id)
        self._transact(frame)

    def _cmd_set(self, args: list[str]):
        if len(args) < 3:
            print("Usage: set <mode> <fan> <temp> [addr]")
            print(f"  Modes: {list(self.protocol.fields['modes'].keys())}")
            print(f"  Fan:   {list(self.protocol.fields['fan'].keys())}")
            return
        mode = args[0].upper()
        fan = args[1].upper()
        temp = int(args[2])
        addr = self._parse_addr(args, 3)
        power = mode != "OFF"
        frame = self.protocol.build_set(
            addr, power=power, mode=mode if power else "FAN",
            fan=fan, temp=temp, master_id=self.master_id,
        )
        self._transact(frame)

    def _cmd_lock(self, args: list[str]):
        addr = self._parse_addr(args, 0)
        frame = self.protocol.build_lock(addr, self.master_id)
        self._transact(frame)

    def _cmd_unlock(self, args: list[str]):
        addr = self._parse_addr(args, 0)
        frame = self.protocol.build_unlock(addr, self.master_id)
        self._transact(frame)

    def _cmd_raw(self, args: list[str]):
        raw_hex = "".join(args)
        data = bytearray.fromhex(raw_hex)
        print(f"  TX ({len(data)}): {hex_dump(data)}")
        self.transport.drain()
        self.transport.send(data)
        raw = self.transport.receive(self.protocol.rx_len + 4)
        if raw:
            print(f"  RX ({len(raw)}): {hex_dump(raw)}")
            rx = self.protocol.find_rx_frame(raw)
            if rx:
                state = self.protocol.parse_rx(rx)
                if state:
                    self._print_state(state)
        else:
            print("  No response.")

    def _cmd_crc(self, args: list[str]):
        raw_hex = "".join(args)
        data = bytearray.fromhex(raw_hex)
        print(f"  Frame ({len(data)}): {hex_dump(data)}")

        # TX CRC
        if len(data) == self.protocol.tx_len:
            crc_pos = self.protocol.tx_offsets["crc"]
            crc = self.protocol.calc_crc(data, crc_pos)
            print(f"  TX CRC at [{crc_pos}] = 0x{crc:02X}")
            data[crc_pos] = crc
            print(f"  Fixed:  {hex_dump(data)}")

        # RX CRC
        elif len(data) == self.protocol.rx_len:
            crc_pos = self.protocol.rx_offsets["crc"]
            crc = self.protocol.calc_crc(data, crc_pos)
            actual = data[crc_pos]
            print(f"  RX CRC at [{crc_pos}]: calculated=0x{crc:02X}, actual=0x{actual:02X}")
            print(f"  Valid: {crc == actual}")

        else:
            # Generic: try all reasonable CRC positions
            for pos in [len(data) - 1, len(data) - 2]:
                crc = self.protocol.calc_crc(data, pos)
                print(f"  CRC at [{pos}] = 0x{crc:02X} (actual 0x{data[pos]:02X}, match={crc == data[pos]})")

    def _cmd_scan(self, args: list[str]):
        start = int(args[0], 0) if len(args) > 0 else 0
        end = int(args[1], 0) if len(args) > 1 else 63
        print(f"Scanning addresses {start}..{end}...")
        found = []
        for addr in range(start, end + 1):
            frame = self.protocol.build_query(addr, self.master_id)
            self.transport.drain()
            self.transport.send(frame)
            raw = self.transport.receive(self.protocol.rx_len + 4)
            if raw:
                rx = self.protocol.find_rx_frame(raw)
                if rx:
                    state = self.protocol.parse_rx(rx)
                    if state:
                        found.append(addr)
                        print(f"  Found device at 0x{addr:02X} ({addr}): mode={state['mode']}, temp={state['setpoint']}°C")
            time.sleep(0.15)  # 130ms time slice per xye spec
        if found:
            print(f"\nDevices found: {[f'0x{a:02X}' for a in found]}")
        else:
            print("\nNo devices found.")

    @staticmethod
    def _print_state(state: dict):
        print("  --- Parsed State ---")
        print(f"  Address:    0x{state['address']:02X} ({state['address']})")
        print(f"  Power:      {'ON' if state['power'] else 'OFF'}")
        print(f"  Mode:       {state['mode']} ({state['mode_raw']})")
        print(f"  Fan:        {state['fan']} (auto={state['fan_auto']}, status_bit={state['fan_status_bit']}, speed={state['fan_speed_bits']}) [{state['fan_raw']}]")
        print(f"  Setpoint:   {state['setpoint']}°C")
        print(f"  Caps:       {state['capabilities']}")
        if state.get("t1") is not None:
            print(f"  T1:         {state['t1']:.1f}°C ({state['t1_raw']})")
        else:
            print(f"  T1:         N/A ({state['t1_raw']})")
        if state.get("t2a") is not None:
            print(f"  T2A:        {state['t2a']:.1f}°C ({state['t2a_raw']})")
        else:
            print(f"  T2A:        N/A ({state['t2a_raw']})")
        if state.get("t2b") is not None:
            print(f"  T2B:        {state['t2b']:.1f}°C ({state['t2b_raw']})")
        else:
            print(f"  T2B:        N/A ({state['t2b_raw']})")
        if state.get("t3") is not None:
            print(f"  T3:         {state['t3']:.1f}°C ({state['t3_raw']})")
        else:
            print(f"  T3:         N/A ({state['t3_raw']})")
        cur = state.get("current")
        print(f"  Current:    {f'{cur}A' if cur is not None else 'N/A'} ({state['current_raw']})")
        print(f"  Flags:      {state['mode_flags']} ({state['mode_flags_raw']})")
        print(f"  Oper:       {state['oper_flags']} ({state['oper_flags_raw']})")
        print(f"  Unk_13:     {state['unknown_13']}")
        print(f"  Unk_14:     {state['unknown_14']}")
        if state["errors"]:
            print(f"  Errors:     {state['errors']}")
        if state["protections"]:
            print(f"  Protections:{state['protections']}")
        if state["ccm_error"]:
            print(f"  CCM Error:  {state['ccm_error']}")
        print(f"  Timer:      start=0x{state['timer_start']:02X}")
        print()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="MDV/Midea XYE fancoil MQTT <-> RS485 agent"
    )
    parser.add_argument("--config", default="schema.yaml",
                        help="Path to schema.yaml (default: schema.yaml)")
    parser.add_argument("--test", action="store_true",
                        help="Interactive test mode (no MQTT)")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    args = parser.parse_args()

    cfg = load_config(args.config)

    log_level = "DEBUG" if args.debug else cfg.get("logging", {}).get("level", "INFO")
    setup_logging(log_level)

    if args.test:
        tester = InteractiveTester(cfg)
        tester.run()
    else:
        agent = Agent(cfg)
        agent.run()


if __name__ == "__main__":
    main()

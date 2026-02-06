#!/usr/bin/env python3
"""
Phase 2 tests: LOCK/UNLOCK, ON/OFF, mode flags (ECO, SWING).
"""

import sys
import time
import yaml
import serial

sys.path.insert(0, ".")
from mqtt_rs485_agent import XYEProtocol, hex_dump

with open("schema.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

proto = XYEProtocol(cfg)
ser_cfg = cfg["serial"]

def open_port():
    return serial.Serial(
        port=ser_cfg["port"],
        baudrate=ser_cfg["baud"],
        bytesize=ser_cfg["bytesize"],
        parity=ser_cfg["parity"],
        stopbits=ser_cfg["stopbits"],
        timeout=ser_cfg.get("read_timeout", 0.5),
    )

def transact(port, frame):
    port.reset_input_buffer()
    port.write(frame)
    print(f"  TX ({len(frame)}): {hex_dump(frame)}")
    time.sleep(0.5)
    raw = port.read(40)
    if raw:
        print(f"  RX ({len(raw)}): {hex_dump(raw)}")
        rx = proto.find_rx_frame(raw)
        if rx:
            return proto.parse_rx(rx)
        print("  [!] No valid frame")
    else:
        print("  [!] No response")
    return None

def pstate(state, label=""):
    if not state:
        print(f"  [{label}] NO RESPONSE"); return
    print(f"  [{label}] pwr={'ON' if state['power'] else 'OFF'} mode={state['mode']}({state['mode_raw']}) "
          f"fan={state['fan']}({state['fan_raw']}) sp={state['setpoint']} "
          f"flags={state['mode_flags']}({state['mode_flags_raw']}) oper={state['oper_flags']}({state['oper_flags_raw']}) "
          f"u13={state['unknown_13']}")

def query(port, label="QUERY"):
    frame = proto.build_query(ser_cfg["address"], master_id=ser_cfg["address"])
    s = transact(port, frame)
    pstate(s, label)
    return s

def set_cmd(port, mode, fan, temp, flags=None, label=None):
    label = label or f"SET {mode} {fan} {temp}"
    frame = proto.build_set(
        ser_cfg["address"], power=True, mode=mode, fan=fan, temp=temp,
        mode_flags=flags, master_id=ser_cfg["address"],
    )
    s = transact(port, frame)
    pstate(s, f"{label}-resp")
    time.sleep(1.0)
    s2 = query(port, f"{label}-after")
    return s2

def set_off(port, label="OFF"):
    """Send power OFF."""
    frame = proto.build_set(
        ser_cfg["address"], power=False, mode="FAN", fan="AUTO", temp=20,
        master_id=ser_cfg["address"],
    )
    s = transact(port, frame)
    pstate(s, f"{label}-resp")
    time.sleep(1.0)
    s2 = query(port, f"{label}-after")
    return s2

port = open_port()
print(f"Port {ser_cfg['port']} opened\n")

# ============================================================
# TEST PHASE 2
# ============================================================

print("\n" + "="*60)
print("PHASE 2A: ON/OFF TEST")
print("="*60)

print("\n--- Step 1: Baseline (should be HEAT AUTO 20) ---")
query(port, "BASELINE")
time.sleep(0.5)

print("\n--- Step 2: Turn OFF ---")
set_off(port)
time.sleep(1.0)

print("\n--- Step 3: Turn ON (HEAT AUTO 22) ---")
set_cmd(port, "HEAT", "AUTO", 22, label="POWER-ON")
time.sleep(0.5)

print("\n" + "="*60)
print("PHASE 2B: MODE FLAGS TEST (ECO, SWING)")
print("="*60)

print("\n--- Step 4: SET HEAT AUTO 22 + ECO ---")
set_cmd(port, "HEAT", "AUTO", 22, flags=["ECO"], label="ECO-ON")
time.sleep(0.5)

print("\n--- Step 5: SET HEAT AUTO 22 (no flags = ECO off) ---")
set_cmd(port, "HEAT", "AUTO", 22, label="ECO-OFF")
time.sleep(0.5)

print("\n--- Step 6: SET HEAT AUTO 22 + SWING ---")
set_cmd(port, "HEAT", "AUTO", 22, flags=["SWING"], label="SWING-ON")
time.sleep(0.5)

print("\n--- Step 7: SET HEAT AUTO 22 (no flags = SWING off) ---")
set_cmd(port, "HEAT", "AUTO", 22, label="SWING-OFF")
time.sleep(0.5)

print("\n" + "="*60)
print("PHASE 2C: LOCK/UNLOCK TEST")
print("="*60)

print("\n--- Step 8: LOCK ---")
frame = proto.build_lock(ser_cfg["address"], master_id=ser_cfg["address"])
s = transact(port, frame)
pstate(s, "LOCK-resp")
time.sleep(1.0)
query(port, "LOCK-after")

print("\n--- Step 9: Query while locked (should still respond) ---")
time.sleep(0.5)
query(port, "LOCKED-Q")

print("\n--- Step 10: UNLOCK ---")
frame = proto.build_unlock(ser_cfg["address"], master_id=ser_cfg["address"])
s = transact(port, frame)
pstate(s, "UNLOCK-resp")
time.sleep(1.0)
query(port, "UNLOCK-after")

print("\n" + "="*60)
print("PHASE 2D: RESTORE TO HEAT AUTO 20")
print("="*60)
set_cmd(port, "HEAT", "AUTO", 20, label="RESTORE")

port.close()
print("\nDone. Port closed.")

#!/usr/bin/env python3
"""
Automated SET command test script for MKG-300C.
Sends a SET command, waits, then queries to verify the result.
"""

import sys
import time
import yaml
import serial

sys.path.insert(0, ".")
from mqtt_rs485_agent import XYEProtocol, hex_dump

# Load config
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

def transact(port, frame, label=""):
    """Send frame, receive response, parse and return state."""
    port.reset_input_buffer()
    port.write(frame)
    print(f"  TX ({len(frame)}): {hex_dump(frame)}")
    time.sleep(0.5)
    raw = port.read(40)
    if raw:
        print(f"  RX ({len(raw)}): {hex_dump(raw)}")
        rx = proto.find_rx_frame(raw)
        if rx:
            state = proto.parse_rx(rx)
            return state
        else:
            print(f"  [!] No valid frame found in response")
    else:
        print(f"  [!] No response received")
    return None

def print_key_state(state, label=""):
    if state is None:
        print(f"  [{label}] No state")
        return
    print(f"  [{label}] power={state['power']}, mode={state['mode']}({state['mode_raw']}), "
          f"fan={state['fan']}({state['fan_raw']}), "
          f"fan_auto={state['fan_auto']}, fan_sbit={state['fan_status_bit']}, fan_spd={state['fan_speed_bits']}, "
          f"setpoint={state['setpoint']}, oper={state['oper_flags']}({state['oper_flags_raw']}), "
          f"unk13={state['unknown_13']}")

def query(port):
    frame = proto.build_query(ser_cfg["address"], master_id=ser_cfg["address"])
    return transact(port, frame, "QUERY")

def set_cmd(port, mode, fan, temp):
    frame = proto.build_set(
        ser_cfg["address"],
        power=True,
        mode=mode,
        fan=fan,
        temp=temp,
        master_id=ser_cfg["address"],
    )
    return transact(port, frame, f"SET {mode} {fan} {temp}")

# ============================================================
# Test sequence
# ============================================================

port = open_port()
print(f"Port {ser_cfg['port']} opened\n")

tests = [
    # (description, mode, fan, temp, delay_before_query)
    ("Baseline query", None, None, None, 0),
    ("SET HEAT AUTO 22", "HEAT", "AUTO", 22, 1.0),
    ("SET HEAT LOW 22", "HEAT", "LOW", 22, 1.0),
    ("SET HEAT MED 22", "HEAT", "MED", 22, 1.0),
    ("SET HEAT HIGH 22", "HEAT", "HIGH", 22, 1.0),
    ("SET HEAT AUTO 22 (restore)", "HEAT", "AUTO", 22, 1.0),
    ("SET COOL AUTO 24", "COOL", "AUTO", 24, 1.0),
    ("SET FAN AUTO 24", "FAN", "AUTO", 24, 1.0),
    ("SET HEAT AUTO 20 (restore)", "HEAT", "AUTO", 20, 1.0),
]

results = []

for i, (desc, mode, fan, temp, delay) in enumerate(tests):
    print(f"\n{'='*60}")
    print(f"TEST {i}: {desc}")
    print(f"{'='*60}")

    if mode is None:
        # Just query
        state = query(port)
        print_key_state(state, "STATE")
        results.append((desc, state))
    else:
        # Send SET
        set_state = set_cmd(port, mode, fan, temp)
        print_key_state(set_state, "SET-RESP")

        # Wait then query
        time.sleep(delay)
        q_state = query(port)
        print_key_state(q_state, "AFTER")
        results.append((desc, q_state))

    time.sleep(0.3)

port.close()

# Summary
print(f"\n\n{'='*60}")
print("SUMMARY")
print(f"{'='*60}")
for desc, state in results:
    if state:
        print(f"  {desc:35s} => mode={state['mode']:6s} fan={state['fan']:7s}({state['fan_raw']}) sp={state['setpoint']} oper={state['oper_flags_raw']} u13={state['unknown_13']}")
    else:
        print(f"  {desc:35s} => NO RESPONSE")

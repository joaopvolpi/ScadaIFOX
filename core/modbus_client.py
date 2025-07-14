# core/modbus_client.py

from pyModbusTCP.client import ModbusClient
import time
import config
import csv
from datetime import datetime
import os

from core.sqlite_helper import save_to_sqlite


# ------------------------------
# Modbus Client Functions
# ------------------------------

def combine_16bit_big_endian(high_word, low_word):
    return (high_word << 16) | (low_word & 0xFFFF)

def read_device_registers(device, register_map):
    client = ModbusClient(
        host=device['ip'],
        port=device['port'],
        unit_id=device['unit_id'],
        auto_open=True,
        timeout = 1.5
    )

    data = {}

    for reg_name, reg_info in register_map.items():
        address = reg_info['address']
        words = reg_info['words']
        multiplier = reg_info.get('multiplier', 1)
        regs = client.read_holding_registers(address, words)

        if regs and len(regs) == words:
            if words == 2:
                raw = combine_16bit_big_endian(regs[0], regs[1])
            else:
                raw = regs[0]

            value = raw * multiplier
            data[reg_name] = round(value, 2)  # Optional: round for nicer output
        else:
            data[reg_name] = None

    return data


def run_poller(store):

    print("Starting Modbus poller...")

    while True:
        for device_name, device in config.DEVICES.items():
            print(f"Polling {device_name} at {device['ip']}:{device['port']}...")
            try:
                result = read_device_registers(device, config.VFD_REGISTER_MAP)

                store[device_name] = result

                if not all(v is None for v in result.values()):
                    save_to_csv(device_name, result)
                    save_to_sqlite(device_name, result)

                print(f"Updated {device_name}: {result}")
                print('-----------------------', flush=True)

            except Exception as e:
                print(f"Error polling {device_name}: {e}")

        time.sleep(config.POLL_INTERVAL)


def save_to_csv(device_name, result, folder="data"):


    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{device_name}.csv")
    timestamp = datetime.now().isoformat()
    fieldnames = ["timestamp"] + list(result.keys())
    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        row = {"timestamp": timestamp}
        row.update(result)
        writer.writerow(row)

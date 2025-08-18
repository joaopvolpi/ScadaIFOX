# core/modbus_client.py

import time
import config

from pyModbusTCP.client import ModbusClient
from datetime import datetime, time as dt_time

from core.sqlite_helper import save_to_sqlite, save_to_csv

def combine_16bit_big_endian(high_word, low_word):
    return (high_word << 16) | (low_word & 0xFFFF)

def read_device_registers(device, register_map):
    client = ModbusClient(
        host=device['ip'],
        port=device['port'],
        unit_id=device['unit_id'],
        auto_open=True,
        timeout=2
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

            # Error handling: descartando valores errados de coleta (evita erros de float...)
            if value > 1_000_000:
                data[reg_name] = None
            else:
                data[reg_name] = round(value, 2)
        else:
            data[reg_name] = None
    return data

def read_device_coils(device, coil_map):
    client = ModbusClient(
        host=device['ip'],
        port=device['port'],
        unit_id=device['unit_id'],
        auto_open=True,
        timeout=2
    )

    data = {}

    for coil_name, coil_info in coil_map.items():
        address = coil_info['address']
        res = client.read_coils(address, 1)
        if res and len(res) == 1:
            data[coil_name] = bool(res[0])
        else:
            data[coil_name] = None
    return data

def poll_device(device_name, device_config, store):
    print(f"Starting poller for {device_name}...")
    while True:
        try:
            # ---------- Verifica se é hora de coleta
            now = datetime.now().time()
            if (dt_time(11, 30) <= now <= dt_time(13, 30)) or (now >= dt_time(17, 0)):
                print(f"[{device_name}] Horário de almoço (11:30 a 13:30) ou fim do expediente (após 17:00) - Coleta interrompida.")
                time.sleep(config.POLL_INTERVAL)
                continue
            # ----------

            all_data = {}
            register_map = device_config.get("register_map")
            coil_map = device_config.get("coil_map")

            if register_map:
                reg_values = read_device_registers(device_config, register_map)
                all_data.update(reg_values)

            if coil_map:
                coil_values = read_device_coils(device_config, coil_map)
                all_data.update(coil_values)

            store[device_name] = all_data

            if not all(v is None for v in all_data.values()): # Se todos os valores forem nulos, não salva
                save_to_csv(device_name, all_data)
                save_to_sqlite(device_name, all_data)

            print(f"[{device_name}] Updated: {all_data}", flush=True)
        except Exception as e:
            print(f"Error polling {device_name}: {e}")

        time.sleep(config.POLL_INTERVAL)
import os
import csv
import config
import sqlite3
import threading

from flask import render_template
from flask import Flask, jsonify, request

from core.sqlite_helper import init_db
from core.modbus_client import poll_device

os.chdir(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
init_db()

# Shared in-memory store for latest polled data
DATA_STORE = {}

# Template Routes

@app.route("/")
def main_menu():
    return render_template("index.html")

@app.route("/masseiras_live")
def masseiras_live():
    return render_template("masseiras_live.html")

@app.route("/masseiras_history")
def masseiras_history():
    return render_template("masseiras_history.html")

@app.route("/tanques_live")
def tanques_live():
    return render_template("tanques_live.html")

@app.route("/tanques_history")
def tanques_history():
    return render_template("tanques_history.html")

# API Endpoints

# @app.route("/device/<device_name>")
# def get_device_data(device_name):
#     """
#     Return the latest data for the given device name.
#     Example: /device/Masseira_1
#     """
#     data = DATA_STORE.get(device_name)
#     if data is None:
#         return jsonify({"error": "Device not found"}), 404
#     return jsonify(data)

@app.route("/api/live")
def get_all_live():
    return jsonify(DATA_STORE)

@app.route("/api/history")
def api_history():
    device = request.args.get("device")
    tags = request.args.getlist("tag")
    start = request.args.get("start")
    end = request.args.get("end")

    conn = sqlite3.connect("data/scada.db")
    c = conn.cursor()
    placeholders = ','.join('?' * len(tags))
    sql = f"""
        SELECT timestamp, tag, value FROM readings
        WHERE device = ?
        AND tag IN ({placeholders})
        AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
    """
    params = [device] + tags + [start, end]

    c.execute(sql, params)
    rows = c.fetchall()
    # print(rows)
    conn.close()

    # Group by tag for frontend
    data = {}
    for ts, tag, value in rows:
        if tag not in data:
            data[tag] = []
        data[tag].append({"timestamp": ts, "value": value})

    return jsonify(data)

@app.route("/api/meta")
def api_meta():
    meta = {}
    # Masseiras
    meta.update({name: {"unit": info["unit"]} for name, info in config.VFD_REGISTER_MAP.items()})
    # Tanques
    meta.update({name: {"unit": info["unit"]} for name, info in config.TANQUES_REGISTER_MAP.items()})
    return jsonify(meta)


if __name__ == "__main__":
    # Start one poller thread per device
    for device_name, device_config in config.DEVICES.items():
        t = threading.Thread(target=poll_device, args=(device_name, device_config, DATA_STORE))
        t.daemon = True
        t.start()

    # Start Flask server
    app.run(
        host=config.FLASK_HOST,
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG    
    )

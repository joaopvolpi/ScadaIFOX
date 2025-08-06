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

@app.route("/acompanhamento_prod")
def acompanhamento_prod():
    return render_template("acompanhamento_prod.html")

# API Endpoints

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


@app.route("/api/overview")
def api_overview():
    period = request.args.get("period", "today")

    mock_data = {
        "period": period,
        "masseiras": {
            "Masseira_1": {
                "energia_kWh": 120.5,
                "corrente_max_A": 32.4,
                "agua_dosada": 1020.0,
                "resina_dosada": 850.0,
                "num_taxadas": 14,
                "tempo_medio_taxada_min": 12.8,
                "energia_por_taxada_kWh": 8.6,
                "horas_operacao": 6.2
            },
            "Masseira_2": {
                "energia_kWh": 115.3,
                "corrente_max_A": 31.0,
                "agua_dosada": 980.0,
                "resina_dosada": 832.0,
                "num_taxadas": 13,
                "tempo_medio_taxada_min": 13.5,
                "energia_por_taxada_kWh": 8.9,
                "horas_operacao": 5.8
            }
        },
        "materias_primas": {
            "resina_dosada": 1750.0,
            "resina_real": 1682.0,
            "agua_dosada": 2040.0,
            "agua_real": 1985.0
        },
        "totais_gerais": {
            "energia_kWh": 235.8,
            "total_taxadas": 27,
            "horas_operacao": 12.0
        }
    }
    return jsonify(mock_data)


@app.route("/api/overview/graph")
def api_overview_graph():
    device = request.args.get("device", "Masseira_1")
    period = request.args.get("period", "today")

    mock_graph_data = {
        "device": device,
        "period": period,
        "frequencia": [
            {"timestamp": "2025-08-06T08:00:00", "value": 47.5},
            {"timestamp": "2025-08-06T08:05:00", "value": 48.0}
        ],
        "corrente": [
            {"timestamp": "2025-08-06T08:00:00", "value": 15.2},
            {"timestamp": "2025-08-06T08:05:00", "value": 16.1}
        ]
    }
    return jsonify(mock_graph_data)


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

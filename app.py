import os
import csv
import config
import sqlite3
import threading

from flask import render_template
from flask import Flask, jsonify, request

from core.sqlite_helper import init_db
from core.modbus_client import poll_device
from core.calculations import *

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
    period = request.args.get("period", "hoje")
    data = calcular_overview(period)
    return jsonify(data)


@app.route("/api/overview/graph")
def api_overview_graph():
    device = request.args.get("device", "Masseira_1")

    now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    start_str = start.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end.strftime("%Y-%m-%dT%H:%M:%S")

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            timestamp,
            MAX(CASE WHEN tag = 'OutputFrequency'   THEN value END) AS OutputFrequency,
            MAX(CASE WHEN tag = 'CurrentMagnitude'  THEN value END) AS CurrentMagnitude
        FROM readings
        WHERE device = ?
          AND tag IN ('OutputFrequency','CurrentMagnitude')
          AND timestamp >= ?
          AND timestamp < ?
        GROUP BY timestamp
        ORDER BY timestamp ASC
    """, (device, start_str, end_str))

    rows = cur.fetchall()
    conn.close()

    def _to_float(x):
        try:
            return float(x) if x is not None else None
        except (TypeError, ValueError):
            return None

    frequencia = []
    corrente = []
    for r in rows:
        ts = r["timestamp"]
        of = _to_float(r["OutputFrequency"])
        cm = _to_float(r["CurrentMagnitude"])
        frequencia.append({"timestamp": ts, "value": of})
        corrente.append({"timestamp": ts, "value": cm})

    return jsonify({
        "device": device,
        "period": "today",
        "frequencia": frequencia,
        "corrente": corrente
    })

@app.route("/api/overview_multi")
def api_overview_multi():
    raw = request.args.get("periods", "").strip()
    if not raw:
        return jsonify({"error": "Missing 'periods' (e.g., 'hoje,7d,mtd,30d,ytd')"}), 400

    # normaliza lista, remove vazios e duplicados mantendo a ordem
    seen, periods = set(), []
    for p in (x.strip() for x in raw.split(",")):
        if p and p not in seen:
            seen.add(p)
            periods.append(p)

    if not periods:
        return jsonify({"error": "No valid periods provided"}), 400

    # tenta usar implementação otimizada (se existir); caso contrário, faz fallback
    data = None
    try:
        # import local para não alterar cabeçalho do arquivo
        data = calcular_overview_multi(periods)
    except Exception:
        data = None

    return jsonify(data)



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

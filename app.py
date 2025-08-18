import os
import config
import sqlite3
import threading

from flask import render_template
from flask import Flask, jsonify, request

from core.sqlite_helper import *
from core.modbus_client import poll_device
from core.calculations import *
from core.report import *

os.chdir(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
init_db()
ensure_indexes() # Apenas deixa as consultas no banco de dados mais rápidas

# Estrutura de dados para armazenar as leituras mais recentes - É compartilhada entre threads
DATA_STORE = {}

# Template Routes

@app.route("/") # Rota principal
def main_menu():
    return render_template("index.html")

@app.route("/masseiras_live") # Rota para visualização ao vivo das masseiras
def masseiras_live():
    return render_template("masseiras_live.html")

@app.route("/masseiras_history") # Rota para histórico das masseiras
def masseiras_history():
    return render_template("masseiras_history.html")

@app.route("/tanques_live") # Rota para visualização ao vivo dos tanques
def tanques_live():
    return render_template("tanques_live.html")

@app.route("/acompanhamento_prod") # Rota para acompanhamento da produção - Métricas, KPIs...
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
    conn.close()

    # Arrumando dados para o front
    data = {}
    for ts, tag, value in rows:
        if tag not in data:
            data[tag] = []
        data[tag].append({"timestamp": ts, "value": value})

    return jsonify(data)

@app.route("/api/meta")
def api_meta(): # Leitura de unidades, metadados...
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

@app.route("/api/overview_multi")
def api_overview_multi():
    """
    Retorna:
    {
      "hoje": {...},
      "7d": {...},
      "mtd": {...},
      "30d": {...},
      "ytd": {...}
    }
    """
    results = gerar_overview_multi()

    return jsonify(results)

@app.route("/relatorios/overview.pdf")
def baixar_relatorio_overview():
    """
    Rota para disparar o download do PDF.
    """
    pdf_bytes = generate_overview_report()
    filename = f"Relatorio_Producao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf", 
        as_attachment=True,
        download_name=filename,
    )

if __name__ == "__main__":
    # Cria uma thread para cada dispositivo no dicionário de configuração  - "DATA_STORE" é compartilhado em todas as threads
    for device_name, device_config in config.DEVICES.items():
        t = threading.Thread(target=poll_device, args=(device_name, device_config, DATA_STORE))
        t.daemon = True # Rodando em segundo plano
        t.start()

    # Start Flask server
    app.run(
        host=config.FLASK_HOST,
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG    
    )

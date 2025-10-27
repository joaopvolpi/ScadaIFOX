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

@app.route("/consumo_diario") # Rota para gráfico diário de consumo
def consumo_diario():
    return render_template("consumo_diario.html")

@app.route("/dosagens")
def dosagens_page():
    return render_template("dosagens.html")


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

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    placeholders = ",".join("?" * len(tags))
    sql = f"""
        SELECT
            (strftime('%s', timestamp) * 1000) AS ts_ms,  -- numeric timestamp (ms)
            tag,
            value
        FROM readings
        WHERE device = ?
          AND tag IN ({placeholders})
          AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
    """
    params = [device] + tags + [start, end]

    c.execute(sql, params)
    rows = c.fetchall()
    conn.close()

    data = {}
    for ts_ms, tag, value in rows:
        data.setdefault(tag, []).append({"t": int(ts_ms), "v": float(value) if value is not None else None})

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
    data_base = request.args.get("data_base", None)
    data = calcular_overview(period, data_base=data_base)
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
    data_base = request.args.get("data_base", None)
    results = gerar_overview_multi(data_base=data_base)

    return jsonify(results)

@app.route("/gerar_relatorio")
def baixar_relatorio_overview():
    """
    Rota para disparar o download do PDF.
    """
    data_base = request.args.get("data_base", None)
    pdf_bytes = generate_overview_report(data_base)
    filename = f"Relatorio_Producao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf", 
        as_attachment=True,
        download_name=filename,
    )

@app.route("/api/daily_tachadas")
def daily_tachadas():
    periodo = request.args.get("periodo", "30d")
    data_base = request.args.get("data_base", None)
    data = gerar_relatorio_diario_masseiras(periodo, data_base=data_base)
    return jsonify(data)

@app.route("/api/dosagens")
def api_dosagens():
    tipo = request.args.get("tipo", None)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    sql = "SELECT timestamp_start, device, tipo, destino, qnt_solicitada, peso_inicio, peso_fim, peso_real FROM dosagens"
    params = []
    if tipo:
        sql += " WHERE tipo = ?"
        params.append(tipo)
    sql += " ORDER BY timestamp_start DESC LIMIT 200"

    c.execute(sql, params)
    rows = c.fetchall()
    conn.close()

    data = [
        {
            "timestamp_start": r[0],
            "device": r[1],
            "tipo": r[2],
            "destino": r[3],
            "qnt_solicitada": r[4],
            "peso_inicio": r[5],
            "peso_fim": r[6],
            "peso_real": r[7],
        }
        for r in rows
    ]
    return jsonify(data)

# ------------------

def background_updater():
    """Runs periodically to detect and store new discharge operations."""
    while True:
        try:
            update_dosagens_table()
        except Exception as e:
            print(f"[dosagens_updater] Error: {e}")

        time.sleep(1)

        try:
            update_masseira_daily()
        except Exception as e:
            print(f"[masseira_updater] Error: {e}")

        time.sleep(3600)  # every 1 hour

if __name__ == "__main__":

    init_db()
    ensure_indexes() # Apenas deixa as consultas no banco de dados mais rápidas
    cleanup_db(30) # Mantém readings apenas com dados recentes

    # Cria uma thread para cada dispositivo no dicionário de configuração  - "DATA_STORE" é compartilhado em todas as threads
    for device_name, device_config in config.DEVICES.items():
        t = threading.Thread(target=poll_device, args=(device_name, device_config, DATA_STORE))
        t.daemon = True # Rodando em segundo plano
        t.start()

    # 2. Start the background updater thread
    t_ops = threading.Thread(target=background_updater, daemon=True)
    t_ops.start()

    # Start Flask server
    app.run(
        host=config.FLASK_HOST,
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG    
    )
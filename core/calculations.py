import sqlite3
from datetime import datetime, timedelta
from config import DB_FILE
from collections import defaultdict
import time

# ================================
# Funções auxiliares
# ================================
def _periodo_para_datas(periodo: str, data_base = None):
    """Retorna intervalo (start, end) baseado no período ('hoje', 'ontem', '7d', '30d', 'mtd', 'ytd')."""
    if data_base:
        end = datetime.strptime(data_base, "%Y-%m-%d")
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    else:
        end = datetime.now()

    if periodo == "hoje":
        start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif periodo == "ontem":
        start = (end - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif periodo == "7d":
        start = end - timedelta(days=7)
    elif periodo == "30d":
        start = end - timedelta(days=30)
    elif periodo == "mtd":  # Month to Date
        start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif periodo == "ytd":  # Year to Date
        start = end.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = end - timedelta(days=7)
    return start.isoformat(), end.isoformat()

# ================================
# KPI Masseiras
# ================================
def calcular_totais_masseiras_sql(start, end):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Cálculo das integrais (para calcular potência em kWh) e tempo de operação (freq das masseiras > 0) direto no SQL
    c.execute("""
        WITH pivot AS (
            SELECT
                device,
                timestamp,
                MAX(CASE WHEN tag='OutputPower' THEN value END) AS power,
                MAX(CASE WHEN tag='OutputFrequency' THEN value END) AS freq,
                MAX(CASE WHEN tag='CurrentMagnitude' THEN value END) AS curr_mag
            FROM readings
            WHERE device IN ('Masseira_1','Masseira_2')
              AND tag IN ('OutputPower','OutputFrequency','CurrentMagnitude')
              AND timestamp BETWEEN ? AND ?
            GROUP BY timestamp, device
        )
        SELECT
            device,
            SUM(prev_power * dt_h) AS energia_kWh,
            SUM(CASE WHEN prev_freq > 0 THEN dt_h ELSE 0 END) AS horas_operacao,
            MAX(curr_mag) AS corrente_max
        FROM (
            SELECT
                device,
                power,
                freq,
                curr_mag,
                LAG(power) OVER (PARTITION BY device ORDER BY timestamp) AS prev_power,
                LAG(freq) OVER (PARTITION BY device ORDER BY timestamp) AS prev_freq,
                (strftime('%s', timestamp) - LAG(strftime('%s', timestamp)) OVER (PARTITION BY device ORDER BY timestamp)) / 3600.0 AS dt_h
            FROM pivot
        )
        GROUP BY device;
    """, (start, end))

    # Apenas monta resultado com valores 0, para garantir consistência no retorno
    result = {"Masseira_1": {"energia_kWh": 0.0, "horas_operacao": 0.0, "corrente_max": 0.0},
              "Masseira_2": {"energia_kWh": 0.0, "horas_operacao": 0.0, "corrente_max": 0.0}}

    for dev, energia, horas, corrente in c.fetchall():
        result[dev] = {
            "energia_kWh": round(energia or 0.0, 2),
            "horas_operacao": round(horas or 0.0, 2),
            "corrente_max": (round(corrente, 2) if corrente is not None else None),
        }

    conn.close()
    return result

# ================================
# Operações de descarga de tanques
# ================================
def calcula_operacoes_descarga_tanques(start, end, tipo):
    """
    Para cada tanque do tipo especificado, retorna:
    {
      "Tanque_X_tipo": {
         "mass1": [ {horario, qnt_solicitada, peso_inicio, peso_fim}, ... ],
         "mass2": [ {horario, qnt_solicitada, peso_inicio, peso_fim}, ... ],
      },
      ...
    }
    A detecção usa a borda 0->1 de 'Operacao em andamento' com
      Descarga selecionada = 1 AND Botão Liga = 1.
    O destino é definido pelas válvulas no instante da borda.
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    tipo = ('Tanque_1_Agua', 'Tanque_2_Agua') if tipo=='Agua' else ('Tanque_1_Resina', 'Tanque_2_Resina')

    c.execute("""
        SELECT
            timestamp,
            device,
            MAX(CASE WHEN tag = 'Descarga selecionada' THEN value END)           AS DescargaSelecionada,
            MAX(CASE WHEN tag = 'Botão Liga' THEN value END)                     AS BotaoLiga,
            MAX(CASE WHEN tag = 'Operacao em andamento' THEN value END)          AS OpAndamento,
            MAX(CASE WHEN tag = 'Valv. Desc. Mass. 1' THEN value END)            AS V1,
            MAX(CASE WHEN tag = 'Valv. Desc. Mass. 2' THEN value END)            AS V2,
            MAX(CASE WHEN tag = 'Qnt. Solicitada (descarga)' THEN value END)     AS QtdSolicDesc,
            MAX(CASE WHEN tag = 'Peso' THEN value END)                           AS Peso
        FROM readings
        WHERE device IN (?, ?)
          AND tag IN ('Descarga selecionada','Operacao em andamento','Botão Liga',
                      'Valv. Desc. Mass. 1','Valv. Desc. Mass. 2',
                      'Qnt. Solicitada (descarga)','Peso')
          AND timestamp BETWEEN ? AND ?
        GROUP BY timestamp, device
        HAVING OpAndamento IS NOT NULL
           AND BotaoLiga IS NOT NULL
           AND DescargaSelecionada IS NOT NULL
           AND V1 IS NOT NULL
           AND V2 IS NOT NULL
           AND Peso IS NOT NULL
           AND QtdSolicDesc IS NOT NULL
        ORDER BY device, timestamp
    """, (tipo[0], tipo[1], start, end))

    # Estruturas
    resultado = defaultdict(lambda: {"Masseira_1": [], "Masseira_2": []})
    last_op = {}                         # estado anterior de OpAndamento por tanque
    operacao_aberta = {}                 # tanque -> dict com dados em curso

    rows = c.fetchall()
    conn.close()

    # -----------------------------------------------------------------------------------
    for i, (ts_str, dev, d_sel, b_liga, op_and, v1, v2, qtd_solic, peso) in enumerate(rows):
        d = int(d_sel)
        b = int(b_liga)
        op = int(op_and)
        v1i = int(v1)
        v2i = int(v2)
        ts = datetime.fromisoformat(ts_str)

        prev = last_op.get(dev, 0)

        # --- Borda de subida (início de operação)
        if prev == 0 and op == 1 and d == 1 and b == 1:
            destino = "Masseira_1" if (v1i == 1 and v2i == 0) else "Masseira_2"
            operacao_aberta[dev] = {
                "inicio_ts": ts_str,
                "destino": destino,
                "qtd_solic_desc": float(qtd_solic),
                "peso_inicio": float(peso),
                "peso_fim": float(peso),
            }

        # --- Durante a operação (op == 1): atualiza peso_fim com última leitura válida
        if dev in operacao_aberta and op == 1:
            operacao_aberta[dev]["peso_fim"] = float(peso)

        # --- Borda de descida (fim de operação)
        if prev == 1 and op == 0:
            # Só processa se realmente houver operação aberta
            if dev not in operacao_aberta:
                # queda órfã: não havia início válido
                last_op[dev] = op
                continue

            alvo = ts + timedelta(seconds=10)
            peso_fim_ajustado = operacao_aberta[dev].get("peso_fim")

            j = i + 1
            while j < len(rows) and rows[j][1] == dev: # coluna 'device'
                ts_j_str = rows[j][0] # coluna 'timestamp'
                peso_j = rows[j][8]  # coluna 'Peso'
                ts_j = datetime.fromisoformat(ts_j_str)
                if ts_j >= alvo:
                    peso_fim_ajustado = float(peso_j)
                    break
                j += 1

            # aplica o peso_fim ajustado (se não achou, mantém o último visto durante a operação)
            operacao_aberta[dev]["peso_fim"] = peso_fim_ajustado
            # --------------------------------------------------------------------------------

            # finaliza a operação
            op_reg = operacao_aberta.pop(dev, None)
            if op_reg:
                resultado[dev][op_reg["destino"]].append({
                    "horario": op_reg["inicio_ts"],
                    "qnt_solicitada": op_reg["qtd_solic_desc"],
                    "peso_inicio": op_reg["peso_inicio"],
                    "peso_fim": op_reg["peso_fim"],
                })

        last_op[dev] = op

    # Se terminar o período com operação aberta, fecha usando último peso visto (não há “+10s” disponíveis)
    for dev, op_reg in list(operacao_aberta.items()):
        resultado[dev][op_reg["destino"]].append({
            "horario": op_reg["inicio_ts"],
            "qnt_solicitada": op_reg["qtd_solic_desc"],
            "peso_inicio": op_reg["peso_inicio"],
            "peso_fim": op_reg["peso_fim"],
        })
        operacao_aberta.pop(dev, None)

    return dict(resultado)

# ================================
# Helpers para agregação de operações
# ================================
def _sum_ops_by_masseira(ops_dict):
    """
    ops_dict = saída de calcula_operacoes_descarga_tanques(..., tipo)
    Retorna:
    {
      "Masseira_1": {"dosada": float, "real": float, "num": int},
      "Masseira_2": {"dosada": float, "real": float, "num": int},
    }
    """
    out = {
        "Masseira_1": {"dosada": 0.0, "real": 0.0, "num": 0},
        "Masseira_2": {"dosada": 0.0, "real": 0.0, "num": 0},
    }

    for tanque, destinos in (ops_dict or {}).items():
        for destino in ("Masseira_1", "Masseira_2"):
            for op in destinos.get(destino, []):
                q = float(op["qnt_solicitada"]) if op["qnt_solicitada"] is not None else 0.0
                p0 = float(op["peso_inicio"]) if op["peso_inicio"] is not None else 0.0
                pf = float(op["peso_fim"]) if op["peso_fim"] is not None else 0.0
                out[destino]["dosada"] += q
                out[destino]["real"] += (p0 - pf)
                out[destino]["num"] += 1
    # arredonda saídas
    for k in out:
        out[k]["dosada"] = round(out[k]["dosada"], 2)
        out[k]["real"] = round(out[k]["real"], 2)
    return out

def _sum_ops_total(ops_dict):
    """
    Totais globais (todas as masseiras).
    Retorna: {"dosada": float, "real": float, "num": int}
    """
    tot = {"dosada": 0.0, "real": 0.0, "num": 0}
    for tanque, destinos in (ops_dict or {}).items():
        for destino, lst in destinos.items():
            for op in lst:
                q = float(op["qnt_solicitada"]) if op["qnt_solicitada"] is not None else 0.0
                p0 = float(op["peso_inicio"]) if op["peso_inicio"] is not None else 0.0
                pf = float(op["peso_fim"]) if op["peso_fim"] is not None else 0.0
                tot["dosada"] += q
                tot["real"] += (p0 - pf)
                tot["num"] += 1
    tot["dosada"] = round(tot["dosada"], 2)
    tot["real"] = round(tot["real"], 2)
    return tot

# ================================
# OVERVIEW (apenas monta o JSON final)
# ================================
def calcular_overview(periodo, data_base=None):
    """
    Monta o JSON final no formato desejado, usando:
      - calcular_totais_masseiras(periodo)
      - calcula_operacoes_descarga_tanques(periodo, 'Resina')
      - calcula_operacoes_descarga_tanques(periodo, 'Agua')
    """
    start, end = _periodo_para_datas(periodo, data_base)

    kpi_m = calcular_totais_masseiras_sql(start, end)

    ops_resina = calcula_operacoes_descarga_tanques(start, end, "Resina")

    ops_agua = calcula_operacoes_descarga_tanques(start, end, "Agua")

    agg_resina_by_m = _sum_ops_by_masseira(ops_resina)

    agg_agua_by_m = _sum_ops_by_masseira(ops_agua)

    tot_resina = _sum_ops_total(ops_resina)

    tot_agua = _sum_ops_total(ops_agua)

    # Montar parte das masseiras
    masseiras_out = {}
    for dev, kpi in kpi_m.items():
        energia = float(kpi.get("energia_kWh", 0.0))
        horas   = float(kpi.get("horas_operacao", 0.0))
        corrmax = kpi.get("corrente_max", None)

        res_m = agg_resina_by_m.get(dev, {"dosada": 0.0, "real": 0.0, "num": 0})
        ag_m  = agg_agua_by_m.get(dev,   {"dosada": 0.0, "real": 0.0, "num": 0})

        num_tachadas = int(res_m["num"])
        tempo_medio_min = (horas * 60.0 / num_tachadas) if num_tachadas > 0 else 0.0
        energia_por_tachada = (energia / num_tachadas) if num_tachadas > 0 else 0.0

        masseiras_out[dev] = {
            "energia_kWh": round(energia, 2),
            "corrente_max_A": (round(float(corrmax), 2) if corrmax is not None else None),
            "agua_dosada": round(float(ag_m["dosada"]), 2),
            "agua_real_dosada": round(float(ag_m["real"]), 2),
            "resina_dosada": round(float(res_m["dosada"]), 2),
            "resina_real_dosada": round(float(res_m["real"]), 2),
            "num_tachadas": num_tachadas,
            "tempo_medio_tachada_min": round(tempo_medio_min, 2),
            "energia_por_tachada_kWh": round(energia_por_tachada, 2),
            "horas_operacao": round(horas, 2),
        }

    # Materiais totais
    materias_primas_out = {
        "resina_dosada": tot_resina["dosada"],
        "resina_real":   tot_resina["real"],
        "agua_dosada":   tot_agua["dosada"],
        "agua_real":     tot_agua["real"],
    }

    # Totais gerais
    energia_total = round(sum(m.get("energia_kWh", 0.0) for m in kpi_m.values()), 2)
    horas_total   = round(sum(m.get("horas_operacao", 0.0) for m in kpi_m.values()), 2)
    total_tachadas = int(tot_resina["num"])

    overview = {
        "data_base": (datetime.now().date().isoformat() if not data_base else data_base),
        "period": periodo,
        "masseiras": masseiras_out,
        "materias_primas": materias_primas_out,
        "totais_gerais": {
            "energia_kWh": energia_total,
            "total_tachadas": total_tachadas,
            "horas_operacao": horas_total,
        },
    }

    return overview

# ================================
# MULTI OVERVIEW
# ================================
def _slice_ops_by_period(ops_dict, start_iso, end_iso):
    """Recorta operações por 'horario' dentro de [start,end], mantendo a estrutura."""
    if not ops_dict:
        return {}
    start_dt = datetime.fromisoformat(start_iso)
    end_dt   = datetime.fromisoformat(end_iso)
    out = {}
    for tanque, destinos in ops_dict.items():
        m1, m2 = [], []
        for destino, lst in destinos.items():
            if not lst:
                continue
            target = m1 if destino == "Masseira_1" else m2
            for op in lst:
                h = op.get("horario")
                if not h:
                    continue
                try:
                    hdt = datetime.fromisoformat(h)
                except Exception:
                    continue
                if start_dt <= hdt <= end_dt:
                    target.append(op)
        if m1 or m2:
            out[tanque] = {"Masseira_1": m1, "Masseira_2": m2}
    return out

def _build_overview_for_period(periodo, ops_resina_all, ops_agua_all, data_base=None):
    """Monta o JSON do overview para um período específico, cortando as operações pré-carregadas."""
    start, end = _periodo_para_datas(periodo, data_base)

    # KPIs dependem da janela -> calcula por período
    kpi_m = calcular_totais_masseiras_sql(start, end)

    # Recortes das operações
    ops_resina_p = _slice_ops_by_period(ops_resina_all, start, end)
    ops_agua_p   = _slice_ops_by_period(ops_agua_all,   start, end)

    # Agregações
    agg_resina_by_m = _sum_ops_by_masseira(ops_resina_p)
    agg_agua_by_m   = _sum_ops_by_masseira(ops_agua_p)
    tot_resina = _sum_ops_total(ops_resina_p)
    tot_agua   = _sum_ops_total(ops_agua_p)

    # Montagem por masseira
    masseiras_out = {}
    for dev, kpi in kpi_m.items():
        energia = float(kpi.get("energia_kWh", 0.0))
        horas   = float(kpi.get("horas_operacao", 0.0))
        corrmax = kpi.get("corrente_max", None)

        res_m = agg_resina_by_m.get(dev, {"dosada": 0.0, "real": 0.0, "num": 0})
        ag_m  = agg_agua_by_m.get(dev,   {"dosada": 0.0, "real": 0.0, "num": 0})

        num_tachadas = int(res_m["num"])
        tempo_medio_min = (horas * 60.0 / num_tachadas) if num_tachadas > 0 else 0.0
        energia_por_tachada = (energia / num_tachadas) if num_tachadas > 0 else 0.0

        masseiras_out[dev] = {
            "energia_kWh": round(energia, 2),
            "corrente_max_A": (round(float(corrmax), 2) if corrmax is not None else None),
            "agua_dosada": round(float(ag_m["dosada"]), 2),
            "agua_real_dosada": round(float(ag_m["real"]), 2),
            "resina_dosada": round(float(res_m["dosada"]), 2),
            "resina_real_dosada": round(float(res_m["real"]), 2),
            "num_tachadas": num_tachadas,
            "tempo_medio_tachada_min": round(tempo_medio_min, 2),
            "energia_por_tachada_kWh": round(energia_por_tachada, 2),
            "horas_operacao": round(horas, 2),
        }

    materias_primas_out = {
        "resina_dosada": tot_resina["dosada"],
        "resina_real":   tot_resina["real"],
        "agua_dosada":   tot_agua["dosada"],
        "agua_real":     tot_agua["real"],
    }

    energia_total = round(sum(m.get("energia_kWh", 0.0) for m in kpi_m.values()), 2)
    horas_total   = round(sum(m.get("horas_operacao", 0.0) for m in kpi_m.values()), 2)
    total_tachadas = int(tot_resina["num"])

    return {
        "data_base": (datetime.now().date().isoformat() if not data_base else data_base),
        "period": periodo,
        "masseiras": masseiras_out,
        "materias_primas": materias_primas_out,
        "totais_gerais": {
            "energia_kWh": energia_total,
            "total_tachadas": total_tachadas,
            "horas_operacao": horas_total,
        },
    }

def gerar_overview_multi(data_base=None):
    """
    Períodos fixos: hoje, 7d, mtd, 30d, ytd.
    - Chama calcula_operacoes_descarga_tanques apenas 2x (para água e resina) no maior período (ytd)
    - Recorta em memória para os demais (com a função de slice)
    - KPIs de masseira calculados por período
    """
    PERIODOS = ["hoje", "7d", "mtd", "30d", "ytd"]
    start_max, end_max = _periodo_para_datas("ytd", data_base=data_base)

    # 2 chamadas ao DB (mais pesadas) no maior período
    ops_resina_all = calcula_operacoes_descarga_tanques(start_max, end_max, "Resina")
    ops_agua_all   = calcula_operacoes_descarga_tanques(start_max, end_max, "Agua")

    # Monta saída para cada período fixo
    out = {}
    for p in PERIODOS:
        out[p] = _build_overview_for_period(p, ops_resina_all, ops_agua_all, data_base=data_base)

    return out

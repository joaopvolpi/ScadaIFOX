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
        start = (end - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif periodo == "30d":
        start = (end - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
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

    # Query the pre-aggregated daily table instead of raw readings
    c.execute("""
        SELECT
            device,
            ROUND(SUM(energia_kWh), 2) AS energia_kWh,
            ROUND(SUM(horas_operacao), 2) AS horas_operacao,
            ROUND(MAX(corrente_max), 2) AS corrente_max
        FROM masseira_daily
        WHERE date BETWEEN date(?) AND date(?)
        GROUP BY device
    """, (start, end))

    # Default output structure
    result = {
        "Masseira_1": {"energia_kWh": 0.0, "horas_operacao": 0.0, "corrente_max": 0.0},
        "Masseira_2": {"energia_kWh": 0.0, "horas_operacao": 0.0, "corrente_max": 0.0},
    }

    for dev, energia, horas, corrente in c.fetchall():
        result[dev] = {
            "energia_kWh": energia or 0.0,
            "horas_operacao": horas or 0.0,
            "corrente_max": corrente or 0.0,
        }

    conn.close()
    return result

# ================================
# Operações de descarga de tanques
# ================================
def calcula_operacoes_descarga_tanques(start, end, tipo): # Função passa a ser usada somente no batch update da tabela operacoes
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

def fetch_operacoes_from_table(start, end, tipo): # consulta da nova tabela operacoes
    """
    Lê operações já registradas na tabela 'dosagens' e retorna no mesmo formato
    de saída usado por calcula_operacoes_descarga_tanques().

    Estrutura de retorno:
    {
      "Tanque_1_<tipo>": {
         "Masseira_1": [ {horario, qnt_solicitada, peso_inicio, peso_fim}, ... ],
         "Masseira_2": [ ... ],
      },
      "Tanque_2_<tipo>": { ... }
    }
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        SELECT
            timestamp_start AS horario,
            device,
            tipo,
            destino,
            qnt_solicitada,
            peso_inicio,
            peso_fim
        FROM dosagens
        WHERE tipo = ?
          AND timestamp_start BETWEEN ? AND ?
        ORDER BY device, horario
    """, (tipo, start, end))

    rows = c.fetchall()
    conn.close()

    # Monta a estrutura no mesmo formato esperado pelas funções de agregação
    resultado = defaultdict(lambda: {"Masseira_1": [], "Masseira_2": []})

    for horario, device, tipo, destino, qnt_solicitada, peso_inicio, peso_fim in rows:
        key = f"{device}_{tipo}"  # mantém compatibilidade com chamadas antigas
        resultado[key][destino].append({
            "horario": horario,
            "qnt_solicitada": qnt_solicitada,
            "peso_inicio": peso_inicio,
            "peso_fim": peso_fim
        })

    return dict(resultado)

# ================================
# Helpers para agregação de operações
# ================================
def _sum_ops_by_masseira(ops_dict):
    """
    ops_dict = saída de fetch_operacoes_from_table(..., tipo)
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
# OVERVIEWS
# ================================
def calcular_overview(periodo, data_base=None):
    """
    Monta o JSON final no formato desejado, usando:
      - calcular_totais_masseiras(periodo)
      - fetch_operacoes_from_table(periodo, 'Resina')
      - fetch_operacoes_from_table(periodo, 'Agua')
    """
    start, end = _periodo_para_datas(periodo, data_base)

    kpi_m = calcular_totais_masseiras_sql(start, end)

    ops_resina = fetch_operacoes_from_table(start, end, "Resina")

    ops_agua = fetch_operacoes_from_table(start, end, "Agua")

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
    - Chama fetch_operacoes_from_table apenas 2x (para água e resina) no maior período (ytd)
    - Recorta em memória para os demais (com a função de slice)
    - KPIs de masseira calculados por período
    """
    PERIODOS = ["hoje", "7d", "mtd", "30d", "ytd"]
    start_max, end_max = _periodo_para_datas("ytd", data_base=data_base)

    # 2 chamadas ao DB (mais pesadas) no maior período
    ops_resina_all = fetch_operacoes_from_table(start_max, end_max, "Resina")
    ops_agua_all   = fetch_operacoes_from_table(start_max, end_max, "Agua")

    # Monta saída para cada período fixo
    out = {}
    for p in PERIODOS:
        out[p] = _build_overview_for_period(p, ops_resina_all, ops_agua_all, data_base=data_base)

    return out

def calcular_tachadas_diarias(periodo: str, data_base=None):
    """
    Calcula e retorna o número de tachadas por dia e por masseira para um dado período.

    Args:
        periodo (str): O período a ser analisado ('hoje', 'ontem', '7d', etc.).
        data_base (str, optional): Data de referência para os cálculos. Defaults to None.

    Returns:
        dict: Um dicionário onde as chaves são as datas (YYYY-MM-DD) e os valores
              são dicionários com a contagem de tachadas para cada masseira.
              Ex: {'2025-08-26': {'Masseira_1': 5, 'Masseira_2': 3}, ...}
    """
    start, end = _periodo_para_datas(periodo, data_base)

    # A função fetch_operacoes_from_table já retorna os dados brutos de todas as operações.
    # Vamos usar as operações de resina, pois a 'tachada' é definida por uma descarga de resina.
    ops_resina = fetch_operacoes_from_table(start, end, "Resina")

    # Estrutura para armazenar a contagem diária
    tachadas_por_dia = defaultdict(lambda: {"Masseira_1": 0, "Masseira_2": 0})

    for tanque, destinos in (ops_resina or {}).items():
        for destino, lista_operacoes in destinos.items():
            for op in lista_operacoes:
                horario_str = op.get("horario")
                if horario_str:
                    try:
                        # Extrai a data da string de horário
                        dia = datetime.fromisoformat(horario_str).date().isoformat()
                        # Incrementa o contador para a masseira e o dia correspondente
                        tachadas_por_dia[dia][destino] += 1
                    except ValueError:
                        # Ignora se o formato da data for inválido
                        continue
    
    # Converte o defaultdict para um dict padrão antes de retornar
    return dict(tachadas_por_dia)

def calcular_kpis_diarios_sql(periodo: str, data_base=None):
    start, end = _periodo_para_datas(periodo, data_base)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        SELECT
            date,
            device,
            ROUND(energia_kWh, 2),
            ROUND(horas_operacao, 2),
            ROUND(corrente_max, 2)
        FROM masseira_daily
        WHERE date BETWEEN date(?) AND date(?)
        ORDER BY date, device;
    """, (start, end))

    result = defaultdict(lambda: {"Masseira_1": {}, "Masseira_2": {}})

    for day, device, energia, horas, corrente in c.fetchall():
        result[day][device] = {
            "energia_kWh": energia,
            "horas_operacao": horas,
            "corrente_max": corrente,
            "num_tachadas": 0,  # é preenchido por outra função
        }

    conn.close()
    return dict(result)

def gerar_relatorio_diario_masseiras(periodo: str, data_base=None):
    """
    Combina os resultados de KPIs e contagem de tachadas em um relatório diário consolidado.

    Args:
        periodo (str): O período a ser analisado.
        data_base (str, optional): Data de referência. Defaults to None.

    Returns:
        dict: Um dicionário com todos os dados consolidados por dia.
              Ex: {'2025-08-26': {'Masseira_1': {'energia_kWh': 100.5, 'num_tachadas': 5, ...}, ...}, ...}
    """
    # 1. Obter os KPIs (energia, horas de operação, etc.)
    kpis_diarios = calcular_kpis_diarios_sql(periodo, data_base)
    
    # 2. Obter a contagem de tachadas
    tachadas_diarias = calcular_tachadas_diarias(periodo, data_base)

    # 3. Consolidar os resultados
    relatorio = defaultdict(lambda: {"Masseira_1": {}, "Masseira_2": {}})

    # Primeiro, preenche com os KPIs diários
    for dia, dados_masseira in kpis_diarios.items():
        relatorio[dia]["Masseira_1"].update(dados_masseira.get("Masseira_1", {}))
        relatorio[dia]["Masseira_2"].update(dados_masseira.get("Masseira_2", {}))
    
    # Em seguida, adiciona a contagem de tachadas
    for dia, dados_tachadas in tachadas_diarias.items():
        if "Masseira_1" in dados_tachadas:
            relatorio[dia]["Masseira_1"]["num_tachadas"] = dados_tachadas["Masseira_1"]
        if "Masseira_2" in dados_tachadas:
            relatorio[dia]["Masseira_2"]["num_tachadas"] = dados_tachadas["Masseira_2"]

    return dict(relatorio)
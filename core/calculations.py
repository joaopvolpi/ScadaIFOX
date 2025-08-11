# analytics_optimized.py

import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from functools import lru_cache

from typing import Dict, List, Tuple

# ================================
# Config
# ================================
from config import DB_FILE

CACHE_TTL_SECONDS = 60  # small in-process TTL for raw-row caches (optional)


# ================================
# Connection + PRAGMAs + Indexes
# ================================
def get_connection() -> sqlite3.Connection:
    """
    Open a connection with read-mostly PRAGMAs applied.
    PRAGMAs are per-connection in SQLite, so we set them here.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    # mmap_size depends on OS limits; it will be clamped if too large.
    try:
        conn.execute("PRAGMA mmap_size = 30000000000;")
    except sqlite3.DatabaseError:
        pass
    return conn


def ensure_indexes(conn: sqlite3.Connection) -> None:
    """
    Create covering/partial indexes to accelerate the exact queries below.
    Idempotent (IF NOT EXISTS).
    """
    cur = conn.cursor()

    # General helpers
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_tag_ts_dev
        ON readings(tag, timestamp, device);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_dev_ts
        ON readings(device, timestamp);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_ts
        ON readings(timestamp);
    """)

    # Partial index for operations query (limits scanned tags)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_ops_tags_ts_dev
        ON readings(tag, timestamp, device)
        WHERE tag IN ('Descarga selecionada','Operacao em andamento','Botão Liga',
                      'Valv. Desc. Mass. 1','Valv. Desc. Mass. 2',
                      'Qnt. Solicitada (descarga)','Peso');
    """)

    # Partial index for masseiras KPI
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_masseiras_tags_ts_dev
        ON readings(tag, timestamp, device)
        WHERE tag IN ('OutputPower','OutputFrequency','CurrentMagnitude');
    """)

    conn.commit()


# Call once at module import (safe & fast even if run multiple times)
try:
    _conn_boot = get_connection()
    ensure_indexes(_conn_boot)
finally:
    try:
        _conn_boot.close()
    except Exception:
        pass


# ================================
# Período helpers
# ================================
def _periodo_para_datas(periodo: str) -> Tuple[str, str]:
    """Mantém assinatura e formato (ISO) para compatibilidade externa."""
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
    elif periodo == "mtd":
        start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif periodo == "ytd":
        start = end.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = end - timedelta(days=7)
    return start.isoformat(), end.isoformat()


def _periodo_para_epochs(periodo: str) -> Tuple[int, int]:
    """Epoch seconds for internal math and fast filtering."""
    s_iso, e_iso = _periodo_para_datas(periodo)
    # Using SQLite strftime in queries; here we just keep epochs for Python filtering if needed.
    s = int(datetime.fromisoformat(s_iso).timestamp())
    e = int(datetime.fromisoformat(e_iso).timestamp())
    return s, e


# ================================
# MASSEIRAS KPI (no datetime parsing in loop)
# ================================
def calcular_totais_masseiras(periodo: str) -> Dict[str, Dict[str, float]]:
    start_iso, end_iso = _periodo_para_datas(periodo)
    conn = get_connection()
    c = conn.cursor()

    # Use epoch seconds to avoid Python datetime parsing per-row.
    c.execute("""
        SELECT
            CAST(strftime('%s', timestamp) AS INTEGER) AS ts,
            device,
            MAX(CASE WHEN tag = 'OutputPower' THEN value END)          AS OutputPower,
            MAX(CASE WHEN tag = 'OutputFrequency' THEN value END)      AS OutputFrequency,
            MAX(CASE WHEN tag = 'CurrentMagnitude' THEN value END)     AS CurrentMagnitude
        FROM readings
        WHERE device LIKE 'Masseira%%'
          AND tag IN ('OutputPower','OutputFrequency','CurrentMagnitude')
          AND timestamp BETWEEN ? AND ?
        GROUP BY device, ts
        ORDER BY device, ts
    """, (start_iso, end_iso))

    energia_por_dev = defaultdict(float)
    horas_por_dev = defaultdict(float)
    corrente_max_por_dev = {}

    last_ts = {}
    last_power = {}
    last_freq = {}

    for ts, dev, power, freq, curr_mag in c:
        if curr_mag is not None:
            cm = float(curr_mag)
            pm = corrente_max_por_dev.get(dev)
            if pm is None or cm > pm:
                corrente_max_por_dev[dev] = cm

        if dev in last_ts:
            dt_h = (ts - last_ts[dev]) / 3600.0
            prev_power = last_power.get(dev)
            if prev_power is not None:
                energia_por_dev[dev] += float(prev_power) * dt_h
            prev_freq = last_freq.get(dev)
            if prev_freq is not None and float(prev_freq) > 0:
                horas_por_dev[dev] += dt_h

        last_ts[dev] = ts
        if power is not None:
            last_power[dev] = float(power)
        if freq is not None:
            last_freq[dev] = float(freq)

    conn.close()

    devices = set(energia_por_dev) | set(horas_por_dev) | set(corrente_max_por_dev)
    result = {
        dev: {
            "energia_kWh": round(energia_por_dev.get(dev, 0.0), 2),
            "horas_operacao": round(horas_por_dev.get(dev, 0.0), 2),
            "corrente_max": (round(corrente_max_por_dev[dev], 2)
                             if dev in corrente_max_por_dev else None),
        }
        for dev in devices
    }
    return result


# ================================
# DESCARGA TANQUES (no datetime parsing in loop)
# ================================
def calcula_operacoes_descarga_tanques(periodo: str, tipo: str):
    """
    Mantém o mesmo output e cálculos do seu código.
    Apenas troca parsing de datetime por epoch.
    """
    start_iso, end_iso = _periodo_para_datas(periodo)
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        SELECT
            CAST(strftime('%s', timestamp) AS INTEGER) AS ts,
            device,
            MAX(CASE WHEN tag = 'Descarga selecionada' THEN value END)           AS DescargaSelecionada,
            MAX(CASE WHEN tag = 'Botão Liga' THEN value END)                     AS BotaoLiga,
            MAX(CASE WHEN tag = 'Operacao em andamento' THEN value END)          AS OpAndamento,
            MAX(CASE WHEN tag = 'Valv. Desc. Mass. 1' THEN value END)            AS V1,
            MAX(CASE WHEN tag = 'Valv. Desc. Mass. 2' THEN value END)            AS V2,
            MAX(CASE WHEN tag = 'Qnt. Solicitada (descarga)' THEN value END)     AS QtdSolicDesc,
            MAX(CASE WHEN tag = 'Peso' THEN value END)                           AS Peso
        FROM readings
        WHERE device LIKE ?
          AND tag IN ('Descarga selecionada','Operacao em andamento','Botão Liga',
                      'Valv. Desc. Mass. 1','Valv. Desc. Mass. 2',
                      'Qnt. Solicitada (descarga)','Peso')
          AND timestamp BETWEEN ? AND ?
        GROUP BY device, ts
        HAVING OpAndamento IS NOT NULL
           AND BotaoLiga IS NOT NULL
           AND Peso IS NOT NULL
        ORDER BY device, ts
    """, (f"%{tipo}", start_iso, end_iso))

    rows = c.fetchall()
    conn.close()

    resultado = defaultdict(lambda: {"Masseira_1": [], "Masseira_2": []})
    last_op = {}
    operacao_aberta = {}

    for ts, dev, d_sel, b_liga, op_and, v1, v2, qtd_solic, peso in rows:
        op = None if op_and is None else int(op_and)
        d = 0 if d_sel is None else int(d_sel)
        b = 0 if b_liga is None else int(b_liga)
        v1i = 0 if v1 is None else int(v1)
        v2i = 0 if v2 is None else int(v2)

        prev = last_op.get(dev, 0)

        # Borda de subida
        if op is not None and prev == 0 and op == 1 and d == 1 and b == 1:
            destino = "Masseira_1" if (v1i == 1 and v2i == 0) else "Masseira_2"
            operacao_aberta[dev] = {
                "inicio_ts": datetime.fromtimestamp(ts).isoformat(),  # mantém formato ISO no output
                "destino": destino,
                "qtd_solic_desc": float(qtd_solic) if qtd_solic is not None else None,
                "peso_inicio": float(peso) if peso is not None else None,
                "peso_fim": float(peso) if peso is not None else None,
            }

        # Durante a operação
        if dev in operacao_aberta and op == 1:
            if peso is not None:
                operacao_aberta[dev]["peso_fim"] = float(peso)

        # Borda de descida
        if op is not None and prev == 1 and op == 0:
            if dev not in operacao_aberta:
                last_op[dev] = op
                continue

            # Mantém exatamente a mesma lógica (olhar +10s usando varredura linear)
            alvo = ts + 10
            peso_fim_ajustado = operacao_aberta[dev].get("peso_fim")

            # Procura próxima leitura do mesmo device com ts >= alvo
            # (mesma lógica de antes: varre na frente)
            # Observação: manter essa parte garante "outputs não mudam".
            idx = 0
            # encontra posição atual de forma simples (mantendo O(n) como original)
            # como não guardamos i, fazemos uma busca linear curta
            # (se quiser, pode passar o índice no loop e usar a mesma técnica de antes)
            for i, r in enumerate(rows):
                if r[0] == ts and r[1] == dev:
                    idx = i
                    break

            j = idx + 1
            while j < len(rows) and rows[j][1] == dev:
                ts_j = rows[j][0]
                peso_j = rows[j][8]
                if ts_j >= alvo:
                    if peso_j is not None:
                        peso_fim_ajustado = float(peso_j)
                    break
                j += 1

            operacao_aberta[dev]["peso_fim"] = peso_fim_ajustado

            op_reg = operacao_aberta.pop(dev, None)
            if op_reg:
                resultado[dev][op_reg["destino"]].append({
                    "horario": op_reg["inicio_ts"],
                    "qnt_solicitada": op_reg["qtd_solic_desc"],
                    "peso_inicio": op_reg["peso_inicio"],
                    "peso_fim": op_reg["peso_fim"],
                })

        if op is not None:
            last_op[dev] = op

    # Fechamento no fim do período
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
# Helpers para agregação (inalterados)
# ================================
def _sum_ops_by_masseira(ops_dict):
    out = {
        "Masseira_1": {"dosada": 0.0, "real": 0.0, "num": 0},
        "Masseira_2": {"dosada": 0.0, "real": 0.0, "num": 0},
    }

    for _, destinos in (ops_dict or {}).items():
        for destino in ("Masseira_1", "Masseira_2"):
            for op in destinos.get(destino, []):
                q = float(op["qnt_solicitada"]) if op["qnt_solicitada"] is not None else 0.0
                p0 = float(op["peso_inicio"]) if op["peso_inicio"] is not None else 0.0
                pf = float(op["peso_fim"]) if op["peso_fim"] is not None else 0.0
                out[destino]["dosada"] += q
                out[destino]["real"] += (p0 - pf)
                out[destino]["num"] += 1
    for k in out:
        out[k]["dosada"] = round(out[k]["dosada"], 2)
        out[k]["real"] = round(out[k]["real"], 2)
    return out


def _sum_ops_total(ops_dict):
    tot = {"dosada": 0.0, "real": 0.0, "num": 0}
    for _, destinos in (ops_dict or {}).items():
        for _, lst in destinos.items():
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
# OVERVIEW (inalterado em formato/resultado)
# ================================
def calcular_overview(periodo: str):
    kpi_m = calcular_totais_masseiras(periodo)
    ops_resina = calcula_operacoes_descarga_tanques(periodo, "Resina")
    ops_agua   = calcula_operacoes_descarga_tanques(periodo, "Agua")

    agg_resina_by_m = _sum_ops_by_masseira(ops_resina)
    agg_agua_by_m   = _sum_ops_by_masseira(ops_agua)

    tot_resina = _sum_ops_total(ops_resina)
    tot_agua   = _sum_ops_total(ops_agua)

    masseiras_out = {}
    for dev, kpi in kpi_m.items():
        energia = float(kpi.get("energia_kWh", 0.0))
        horas   = float(kpi.get("horas_operacao", 0.0))
        corrmax = kpi.get("corrente_max", None)

        res_m = agg_resina_by_m.get(dev, {"dosada": 0.0, "real": 0.0, "num": 0})
        ag_m  = agg_agua_by_m.get(dev,   {"dosada": 0.0, "real": 0.0, "num": 0})

        num_taxadas = int(res_m["num"])
        tempo_medio_min = (horas * 60.0 / num_taxadas) if num_taxadas > 0 else 0.0
        energia_por_taxada = (energia / num_taxadas) if num_taxadas > 0 else 0.0

        masseiras_out[dev] = {
            "energia_kWh": round(energia, 2),
            "corrente_max_A": (round(float(corrmax), 2) if corrmax is not None else None),
            "agua_dosada": round(float(ag_m["dosada"]), 2),
            "agua_real_dosada": round(float(ag_m["real"]), 2),
            "resina_dosada": round(float(res_m["dosada"]), 2),
            "resina_real_dosada": round(float(res_m["real"]), 2),
            "num_taxadas": num_taxadas,
            "tempo_medio_taxada_min": round(tempo_medio_min, 2),
            "energia_por_taxada_kWh": round(energia_por_taxada, 2),
            "horas_operacao": round(horas, 2),
        }

    energia_total = round(sum(m.get("energia_kWh", 0.0) for m in kpi_m.values()), 2)
    horas_total   = round(sum(m.get("horas_operacao", 0.0) for m in kpi_m.values()), 2)
    total_taxadas = int(tot_resina["num"])

    overview = {
        "period": periodo,
        "masseiras": masseiras_out,
        "materias_primas": {
            "resina_dosada": tot_resina["dosada"],
            "resina_real":   tot_resina["real"],
            "agua_dosada":   tot_agua["dosada"],
            "agua_real":     tot_agua["real"],
        },
        "totais_gerais": {
            "energia_kWh": energia_total,
            "total_taxadas": total_taxadas,
            "horas_operacao": horas_total,
        },
    }
    return overview


# ================================
# (4) Reuso/stream: calcular vários períodos com UMA leitura
# ================================
def calcular_overview_multi(periodos: List[str]) -> Dict[str, dict]:
    """
    Lê cada fonte (KPI e operações por tipo) uma única vez no maior intervalo
    e reusa as linhas em memória para montar cada período.
    Output por período é idêntico ao de calcular_overview(periodo).
    """
    if not periodos:
        return {}

    # Pega limites globais
    bounds_iso = [_periodo_para_datas(p) for p in periodos]
    base_start_iso = min(b[0] for b in bounds_iso)
    base_end_iso   = max(b[1] for b in bounds_iso)

    # ---- Fetch KPI (base) ----
    kpi_rows = _fetch_kpi_rows(base_start_iso, base_end_iso)
    # bucket por período
    period_bounds_epoch = {p: _periodo_para_epochs(p) for p in periodos}

    # Pré-agrupa por device e ts (já em epoch), igual à consulta original:
    # kpi_rows: list of (ts, device, power, freq, curr_mag)
    def kpi_for_period(period: str):
        s_ep, e_ep = period_bounds_epoch[period]
        energia_por_dev = defaultdict(float)
        horas_por_dev = defaultdict(float)
        corrente_max_por_dev = {}

        last_ts = {}
        last_power = {}
        last_freq = {}

        # Itera em ordem (device, ts) já garantida pelo fetch
        for ts, dev, power, freq, curr_mag in kpi_rows:
            if ts < s_ep or ts > e_ep:
                continue

            if curr_mag is not None:
                cm = float(curr_mag)
                pm = corrente_max_por_dev.get(dev)
                if pm is None or cm > pm:
                    corrente_max_por_dev[dev] = cm

            if dev in last_ts:
                dt_h = (ts - last_ts[dev]) / 3600.0
                prev_power = last_power.get(dev)
                if prev_power is not None:
                    energia_por_dev[dev] += float(prev_power) * dt_h
                prev_freq = last_freq.get(dev)
                if prev_freq is not None and float(prev_freq) > 0:
                    horas_por_dev[dev] += dt_h

            last_ts[dev] = ts
            if power is not None:
                last_power[dev] = float(power)
            if freq is not None:
                last_freq[dev] = float(freq)

        devices = set(energia_por_dev) | set(horas_por_dev) | set(corrente_max_por_dev)
        return {
            dev: {
                "energia_kWh": round(energia_por_dev.get(dev, 0.0), 2),
                "horas_operacao": round(horas_por_dev.get(dev, 0.0), 2),
                "corrente_max": (round(corrente_max_por_dev[dev], 2)
                                 if dev in corrente_max_por_dev else None),
            }
            for dev in devices
        }

    # ---- Fetch OPS (base) para Resina e Água ----
    ops_rows_resina = _fetch_ops_rows(base_start_iso, base_end_iso, "Resina")
    ops_rows_agua   = _fetch_ops_rows(base_start_iso, base_end_iso, "Agua")

    def ops_for_period(ops_rows, period: str):
        s_ep, e_ep = period_bounds_epoch[period]
        resultado = defaultdict(lambda: {"Masseira_1": [], "Masseira_2": []})
        last_op = {}
        operacao_aberta = {}

        # Filtra em tempo real por período (mantendo ordem e cálculos)
        for ts, dev, d_sel, b_liga, op_and, v1, v2, qtd_solic, peso in ops_rows:
            if ts < s_ep or ts > e_ep:
                continue

            op = None if op_and is None else int(op_and)
            d = 0 if d_sel is None else int(d_sel)
            b = 0 if b_liga is None else int(b_liga)
            v1i = 0 if v1 is None else int(v1)
            v2i = 0 if v2 is None else int(v2)

            prev = last_op.get(dev, 0)

            if op is not None and prev == 0 and op == 1 and d == 1 and b == 1:
                destino = "Masseira_1" if (v1i == 1 and v2i == 0) else "Masseira_2"
                operacao_aberta[dev] = {
                    "inicio_ts": datetime.fromtimestamp(ts).isoformat(),
                    "destino": destino,
                    "qtd_solic_desc": float(qtd_solic) if qtd_solic is not None else None,
                    "peso_inicio": float(peso) if peso is not None else None,
                    "peso_fim": float(peso) if peso is not None else None,
                }

            if dev in operacao_aberta and op == 1:
                if peso is not None:
                    operacao_aberta[dev]["peso_fim"] = float(peso)

            if op is not None and prev == 1 and op == 0:
                if dev not in operacao_aberta:
                    last_op[dev] = op
                    continue

                alvo = ts + 10
                peso_fim_ajustado = operacao_aberta[dev].get("peso_fim")

                # procura na base (ops_rows) a próxima do mesmo device com ts >= alvo
                # mantendo a mesma lógica (linear forward scan)
                # para simplicidade, fazemos nova varredura curta; idem ao single-period
                started = False
                for r in ops_rows:
                    if r[1] == dev and r[0] == ts:
                        started = True
                        continue
                    if not started:
                        continue
                    if r[1] != dev:
                        break
                    ts_j, _, _, _, _, _, _, _, peso_j = r
                    if ts_j >= alvo:
                        if peso_j is not None:
                            peso_fim_ajustado = float(peso_j)
                        break

                operacao_aberta[dev]["peso_fim"] = peso_fim_ajustado

                op_reg = operacao_aberta.pop(dev, None)
                if op_reg:
                    resultado[dev][op_reg["destino"]].append({
                        "horario": op_reg["inicio_ts"],
                        "qnt_solicitada": op_reg["qtd_solic_desc"],
                        "peso_inicio": op_reg["peso_inicio"],
                        "peso_fim": op_reg["peso_fim"],
                    })

            if op is not None:
                last_op[dev] = op

        for dev, op_reg in list(operacao_aberta.items()):
            resultado[dev][op_reg["destino"]].append({
                "horario": op_reg["inicio_ts"],
                "qnt_solicitada": op_reg["qtd_solic_desc"],
                "peso_inicio": op_reg["peso_inicio"],
                "peso_fim": op_reg["peso_fim"],
            })
            operacao_aberta.pop(dev, None)

        return dict(resultado)

    # Monta cada período reaproveitando as leituras base
    out = {}
    for p in periodos:
        kpi = kpi_for_period(p)
        ops_res = ops_for_period(ops_rows_resina, p)
        ops_agu = ops_for_period(ops_rows_agua, p)

        agg_res_by_m = _sum_ops_by_masseira(ops_res)
        agg_agu_by_m = _sum_ops_by_masseira(ops_agu)
        tot_res = _sum_ops_total(ops_res)
        tot_agu = _sum_ops_total(ops_agu)

        masseiras_out = {}
        for dev, kpi_dev in kpi.items():
            energia = float(kpi_dev.get("energia_kWh", 0.0))
            horas   = float(kpi_dev.get("horas_operacao", 0.0))
            corrmax = kpi_dev.get("corrente_max", None)

            res_m = agg_res_by_m.get(dev, {"dosada": 0.0, "real": 0.0, "num": 0})
            ag_m  = agg_agu_by_m.get(dev,   {"dosada": 0.0, "real": 0.0, "num": 0})

            num_taxadas = int(res_m["num"])
            tempo_medio_min = (horas * 60.0 / num_taxadas) if num_taxadas > 0 else 0.0
            energia_por_taxada = (energia / num_taxadas) if num_taxadas > 0 else 0.0

            masseiras_out[dev] = {
                "energia_kWh": round(energia, 2),
                "corrente_max_A": (round(float(corrmax), 2) if corrmax is not None else None),
                "agua_dosada": round(float(ag_m["dosada"]), 2),
                "agua_real_dosada": round(float(ag_m["real"]), 2),
                "resina_dosada": round(float(res_m["dosada"]), 2),
                "resina_real_dosada": round(float(res_m["real"]), 2),
                "num_taxadas": num_taxadas,
                "tempo_medio_taxada_min": round(tempo_medio_min, 2),
                "energia_por_taxada_kWh": round(energia_por_taxada, 2),
                "horas_operacao": round(horas, 2),
            }

        energia_total = round(sum(m.get("energia_kWh", 0.0) for m in kpi.values()), 2)
        horas_total   = round(sum(m.get("horas_operacao", 0.0) for m in kpi.values()), 2)
        total_taxadas = int(tot_res["num"])

        out[p] = {
            "period": p,
            "masseiras": masseiras_out,
            "materias_primas": {
                "resina_dosada": tot_res["dosada"],
                "resina_real":   tot_res["real"],
                "agua_dosada":   tot_agu["dosada"],
                "agua_real":     tot_agu["real"],
            },
            "totais_gerais": {
                "energia_kWh": energia_total,
                "total_taxadas": total_taxadas,
                "horas_operacao": horas_total,
            },
        }

    return out


# ================================
# Raw-row fetchers with tiny TTL cache
# (used by calcular_overview_multi)
# ================================
def _fetch_kpi_rows(start_iso: str, end_iso: str):
    """
    Returns list of tuples (ts_epoch, device, power, freq, curr_mag) ordered by (device, ts).
    """
    key = ("kpi", start_iso, end_iso)
    rows = _cache_get(key)
    if rows is not None:
        return rows

    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT
            CAST(strftime('%s', timestamp) AS INTEGER) AS ts,
            device,
            MAX(CASE WHEN tag = 'OutputPower' THEN value END)          AS OutputPower,
            MAX(CASE WHEN tag = 'OutputFrequency' THEN value END)      AS OutputFrequency,
            MAX(CASE WHEN tag = 'CurrentMagnitude' THEN value END)     AS CurrentMagnitude
        FROM readings
        WHERE device LIKE 'Masseira%%'
          AND tag IN ('OutputPower','OutputFrequency','CurrentMagnitude')
          AND timestamp BETWEEN ? AND ?
        GROUP BY device, ts
        ORDER BY device, ts
    """, (start_iso, end_iso))
    rows = c.fetchall()
    conn.close()

    _cache_set(key, rows)
    return rows


def _fetch_ops_rows(start_iso: str, end_iso: str, tipo: str):
    """
    Returns list of tuples (ts_epoch, device, DescSel, Liga, Op, V1, V2, Qtd, Peso) ordered by (device, ts).
    """
    key = ("ops", tipo, start_iso, end_iso)
    rows = _cache_get(key)
    if rows is not None:
        return rows

    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT
            CAST(strftime('%s', timestamp) AS INTEGER) AS ts,
            device,
            MAX(CASE WHEN tag = 'Descarga selecionada' THEN value END)           AS DescargaSelecionada,
            MAX(CASE WHEN tag = 'Botão Liga' THEN value END)                     AS BotaoLiga,
            MAX(CASE WHEN tag = 'Operacao em andamento' THEN value END)          AS OpAndamento,
            MAX(CASE WHEN tag = 'Valv. Desc. Mass. 1' THEN value END)            AS V1,
            MAX(CASE WHEN tag = 'Valv. Desc. Mass. 2' THEN value END)            AS V2,
            MAX(CASE WHEN tag = 'Qnt. Solicitada (descarga)' THEN value END)     AS QtdSolicDesc,
            MAX(CASE WHEN tag = 'Peso' THEN value END)                           AS Peso
        FROM readings
        WHERE device LIKE ?
          AND tag IN ('Descarga selecionada','Operacao em andamento','Botão Liga',
                      'Valv. Desc. Mass. 1','Valv. Desc. Mass. 2',
                      'Qnt. Solicitada (descarga)','Peso')
          AND timestamp BETWEEN ? AND ?
        GROUP BY device, ts
        HAVING OpAndamento IS NOT NULL
           AND BotaoLiga IS NOT NULL
           AND Peso IS NOT NULL
        ORDER BY device, ts
    """, (f"%{tipo}", start_iso, end_iso))
    rows = c.fetchall()
    conn.close()

    _cache_set(key, rows)
    return rows


# ================================
# Tiny in-process TTL cache
# ================================
_cache_store: Dict[Tuple, Tuple[float, object]] = {}


def _cache_get(key):
    rec = _cache_store.get(key)
    if not rec:
        return None
    ts, val = rec
    if (datetime.now().timestamp() - ts) > CACHE_TTL_SECONDS:
        _cache_store.pop(key, None)
        return None
    return val


def _cache_set(key, val):
    _cache_store[key] = (datetime.now().timestamp(), val)

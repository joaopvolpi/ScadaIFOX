# requirements:
#   pip install requests reportlab flask

from __future__ import annotations
import io
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
)
from reportlab.pdfgen.canvas import Canvas
from flask import Flask, send_file, current_app
from core.calculations import gerar_overview_multi

# =========================
# Utilitários de formatação
# =========================
def _fmt(x: Optional[float], nd=2) -> str:
    if x is None:
        return "—"
    try:
        xf = float(x)
        if xf.is_integer():
            return f"{int(xf):,}".replace(",", ".")
        else:
            return f"{xf:,.{nd}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "—"

def _h2(text: str) -> Paragraph:
    return Paragraph(text, ParagraphStyle(
        name="H2",
        fontName="Helvetica-Bold",
        fontSize=13,
        spaceAfter=4,
        leading=16
    ))

def _h3(text: str) -> Paragraph:
    return Paragraph(text, ParagraphStyle(
        name="H3",
        fontName="Helvetica-Bold",
        fontSize=11,
        spaceAfter=2,
        leading=14
    ))

def _body(text: str) -> Paragraph:
    return Paragraph(text, ParagraphStyle(
        name="Body",
        fontName="Helvetica",
        fontSize=9.5,
        leading=12
    ))

# =========================
# Construção de tabelas
# =========================
def _table(data: List[List[Any]], col_widths: Optional[List[int]] = None) -> Table:
    t = Table(data, colWidths=col_widths, hAlign="CENTER")  # <- CENTRALIZA (ALTERADO)
    style_cmds = [
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.black),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#f7f7f7"), colors.white]),
    ]
    t.setStyle(TableStyle(style_cmds))
    return t

def _make_totais_gerais_table(tg: Dict[str, Any]) -> Table:
    data = [
        ["Energia (kWh)", "Horas de operação", "Total de tachadas"],
        [_fmt(tg.get("energia_kWh")), _fmt(tg.get("horas_operacao")), _fmt(tg.get("total_tachadas"), 0)],
    ]
    return _table(data, col_widths=[50*mm, 50*mm, 50*mm])

def _make_materias_primas_table(mp: Dict[str, Any]) -> Table:
    data = [
        ["Água dosada", "Água real", "Resina dosada", "Resina real"],
        [_fmt(mp.get("agua_dosada")), _fmt(mp.get("agua_real")),
         _fmt(mp.get("resina_dosada")), _fmt(mp.get("resina_real"))],
    ]
    return _table(data, col_widths=[45*mm, 45*mm, 45*mm, 45*mm])

# -------- NOVO: Tabela transposta das masseiras --------
def _make_masseiras_table_transposed(masseiras: Dict[str, Dict[str, Any]]) -> Table:
    # Colunas = nomes das masseiras; Linhas = métricas
    nomes = sorted(masseiras.keys())
    cab = ["Métrica"] + nomes

    metric_map = [
        ("Água dosada", "agua_dosada"),
        ("Água real dosada", "agua_real_dosada"),
        ("Resina dosada", "resina_dosada"),
        ("Resina real dosada", "resina_real_dosada"),
        ("Energia (kWh)", "energia_kWh"),
        ("Horas op.", "horas_operacao"),
        ("Corrente máx (A)", "corrente_max_A"),
        ("Nº tachadas", "num_tachadas"),
        ("kWh por tachada", "energia_por_tachada_kWh"),
        ("Tempo médio (min)", "tempo_medio_tachada_min"),
    ]

    rows: List[List[Any]] = [cab]
    for label, key in metric_map:
        row = [label]
        for n in nomes:
            row.append(_fmt(masseiras.get(n, {}).get(key), 2 if "tachada" not in key and "Nº" not in label else 0))
        rows.append(row)

    # Larguras: 60mm para a coluna de métrica, demais dividem o espaço disponível
    # Em A4 e margens definidas abaixo, 3-4 colunas cabem bem.
    col_widths = [60*mm] + [40*mm for _ in nomes]
    return _table(rows, col_widths=col_widths)
# --------------------------------------------------------

# =========================
# Header em todas as páginas (NOVO)
# =========================
def _resolve_logo_path() -> Optional[str]:
    # tenta achar /static/logo.png no contexto Flask; cai para caminho relativo
    try:
        base = current_app.root_path  # funciona se houver app context
        p = os.path.join(base, "static", "logo.png")
        if os.path.exists(p):
            return p
    except RuntimeError:
        pass
    p = os.path.join("static", "logo.png")
    return p if os.path.exists(p) else None

def _draw_header(canvas: Canvas, doc: SimpleDocTemplate, title: str = ""):
    width, height = doc.pagesize
    bar_h = 18 * mm
    canvas.saveState()

    # Faixa preta no topo
    canvas.setFillColor(colors.black)
    canvas.rect(0, height - bar_h, width, bar_h, stroke=0, fill=1)

    # Logo no canto superior esquerdo
    logo_path = _resolve_logo_path()
    if logo_path:
        try:
            # 26 mm de largura, altura proporcional; posiciona com pequena margem
            logo_w = 40 * mm
            logo_y = height - bar_h + (bar_h - 10*mm) / 2
            canvas.drawImage(
                logo_path,
                doc.leftMargin,  # canto esquerdo dentro da margem
                logo_y,
                width=logo_w,
                preserveAspectRatio=True,
                mask="auto",
                anchor='sw'
            )
        except Exception:
            pass

    canvas.restoreState()

# =========================
# PDF
# =========================
def build_overview_report_pdf(data: Dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    # Aumenta a margem superior para acomodar o header preto (NOVO)
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        rightMargin=14*mm,
        leftMargin=4*mm,
        topMargin=30*mm,   # antes era ~16mm
        bottomMargin=16*mm,
        title="Relatório de Produção - Visão Geral",
        author="SCADA",
    )

    story: List[Any] = []
    styles = getSampleStyleSheet()

    # Cabeçalho textual do relatório (corpo)
    story.append(Paragraph("Relatório de Produção - Visão Geral", ParagraphStyle(
        name="H1",
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=20,
        spaceAfter=6
    )))
    story.append(_body(f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"))
    story.append(Spacer(1, 6*mm))

    period_order = ["hoje", "7d", "30d", "mtd", "ytd"]
    periods = [p for p in period_order if p in data] + [p for p in data.keys() if p not in period_order]

    first = True
    for period in periods:
        bloco = data.get(period, {})
        if not bloco:
            continue

        if not first:
            story.append(PageBreak())
        first = False

        story.append(_h2(f"Período: {period.upper()}"))
        story.append(Spacer(1, 2*mm))

        # Totais gerais
        tg = bloco.get("totais_gerais", {})
        story.append(_h3("Totais gerais"))
        story.append(_make_totais_gerais_table(tg))
        story.append(Spacer(1, 4*mm))

        # Matérias-primas
        mp = bloco.get("materias_primas", {})
        story.append(_h3("Matérias-primas"))
        story.append(_make_materias_primas_table(mp))
        story.append(Spacer(1, 4*mm))

        # Masseiras (TRANSPOSTA)
        mass = bloco.get("masseiras", {})
        if mass:
            story.append(_h3("Métricas por masseira"))
            story.append(_make_masseiras_table_transposed(mass))
            story.append(Spacer(1, 2*mm))

    # Aplica o header em todas as páginas
    doc.build(
        story,
        onFirstPage=lambda canv, d: _draw_header(canv, d),
        onLaterPages=lambda canv, d: _draw_header(canv, d),
    )
    buf.seek(0)
    return buf.read()

def generate_overview_report() -> bytes:
    data = gerar_overview_multi()
    return build_overview_report_pdf(data)

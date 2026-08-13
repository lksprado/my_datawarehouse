"""Formatação pt-BR e escape de HTML.

Tudo que sai no PDF passa por aqui: moeda com ponto de milhar e vírgula
decimal, percentual com vírgula, data em dd/mm/aaaa e mês por extenso.
"""
from __future__ import annotations

from datetime import datetime

MESES = ["", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
         "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
MES_CURTO = ["", "jan", "fev", "mar", "abr", "mai", "jun",
             "jul", "ago", "set", "out", "nov", "dez"]


def brl(v, casas=0):
    if v is None:
        return "—"
    v = float(v)
    if casas == 0:
        s = f"{abs(v):,.0f}".replace(",", ".")
    else:
        s = f"{abs(v):,.{casas}f}".replace(",", "\x00").replace(".", ",").replace("\x00", ".")
    return f"{'-' if v < 0 else ''}R$ {s}"


def pct(v, casas=1):
    return "—" if v is None else f"{float(v):.{casas}f}%".replace(".", ",")


def sinal(v, casas=1, sufixo="%"):
    if v is None:
        return "—"
    # A vírgula decimal vale só para o número. Aplicada à string inteira, como
    # era antes, o sufixo " p.p." saía " p,p," no PDF.
    return (f"{'+' if v >= 0 else ''}{float(v):.{casas}f}".replace(".", ",")
            + sufixo)


def num(v, casas=3):
    """Número solto com vírgula decimal — índices de benchmark, kWh/dia."""
    return "—" if v is None else f"{float(v):.{casas}f}".replace(".", ",")


def data_br(s):
    if not s:
        return "—"
    d = datetime.strptime(s[:10], "%Y-%m-%d").date()
    return d.strftime("%d/%m/%Y")


def dia_mes_br(s):
    """dd/mm — para o corte do meio de mês, onde o ano é redundante."""
    if not s:
        return "—"
    return datetime.strptime(s[:10], "%Y-%m-%d").date().strftime("%d/%m")


def mes_extenso(s):
    d = datetime.strptime(s[:10], "%Y-%m-%d").date()
    return f"{MESES[d.month]} de {d.year}"


def mes_curto(s):
    d = datetime.strptime(s[:10], "%Y-%m-%d").date()
    return f"{MES_CURTO[d.month]}/{str(d.year)[2:]}"


def esc(t):
    return (str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def classe_delta(v, bom_se_sobe=True):
    if v is None or abs(v) < 0.05:
        return ""
    positivo = v > 0 if bom_se_sobe else v < 0
    return "delta-pos" if positivo else "delta-neg"

"""Agregados usados pelos dois relatórios."""
from __future__ import annotations


def media(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def mediana(vals):
    """Usada para dimensionar a reserva e para projetar o fechamento do mês:
    a política pede mediana justamente para que um mês atípico (IPVA, viagem,
    reforma) não infle a meta nem a projeção."""
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return 0.0
    meio = len(vals) // 2
    if len(vals) % 2:
        return float(vals[meio])
    return (float(vals[meio - 1]) + float(vals[meio])) / 2


def variacao(atual, base):
    return None if not base else (atual - base) / base * 100

"""Gráficos SVG inline, sem biblioteca.

Regras herdadas da skill `dataviz` e válidas para os dois relatórios:

- todo gráfico tem **tabela irmã** na mesma seção — a paleta tem slots abaixo
  de 3:1 de contraste na superfície clara e a tabela é a mitigação exigida;
- rótulo direto na ponta da série, em vez de legenda que obriga o olho a
  voltar;
- eixo único, cor por entidade e não por ranking;
- nada de recurso externo: o Chrome roda sem rede em `html_para_pdf.sh`.
"""
from __future__ import annotations

from .formato import esc, mes_curto


def _eixo_y(x0, x1, y_de, vmax, passos=4):
    p = []
    for i in range(passos + 1):
        v = vmax * i / passos
        y = y_de(v)
        p.append(f'<line class="grade" x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}"/>')
        rot = f"{v/1000:.0f}k" if vmax >= 10000 else f"{v:.0f}"
        p.append(f'<text class="eixo" x="{x0-4}" y="{y+3:.1f}" text-anchor="end">{rot}</text>')
    return "".join(p)


def barras_empilhadas(meses, series, largura=640, altura=210):
    """series: lista de (rotulo, cor, [valores]) — uma pilha por mês."""
    m = {"e": 40, "d": 8, "t": 10, "b": 26}
    pw, ph = largura - m["e"] - m["d"], altura - m["t"] - m["b"]
    totais = [sum(s[2][i] for s in series) for i in range(len(meses))]
    vmax = max(totais) * 1.08 or 1
    def y_de(v): return m["t"] + ph - (v / vmax) * ph
    lb = largura / max(len(meses), 1) * 0.58
    passo = pw / max(len(meses), 1)

    p = [f'<svg viewBox="0 0 {largura} {altura}" role="img">']
    p.append(_eixo_y(m["e"], largura - m["d"], y_de, vmax))
    for i, mes in enumerate(meses):
        cx = m["e"] + passo * (i + 0.5)
        acum = 0.0
        for _, cor, vals in series:
            v = vals[i]
            if v <= 0:
                continue
            y0, y1 = y_de(acum + v), y_de(acum)
            p.append(f'<rect class="fatia" x="{cx-lb/2:.1f}" y="{y0:.1f}" '
                     f'width="{lb:.1f}" height="{max(y1-y0,0.6):.1f}" fill="{cor}"/>')
            acum += v
        p.append(f'<text class="eixo" x="{cx:.1f}" y="{altura-13}" '
                 f'text-anchor="middle">{mes_curto(mes)}</text>')
        p.append(f'<text class="rotulo-direto" x="{cx:.1f}" y="{y_de(acum)-3:.1f}" '
                 f'text-anchor="middle">{totais[i]/1000:.1f}k</text>')
    p.append(f'<line class="linha-base" x1="{m["e"]}" y1="{y_de(0):.1f}" '
             f'x2="{largura-m["d"]}" y2="{y_de(0):.1f}"/>')
    p.append("</svg>")
    return "".join(p)


def barras_agrupadas(meses, series, largura=640, altura=200, rotulo_x=None):
    """series: lista de (rotulo, cor, [valores]).

    `rotulo_x` troca a formatação do eixo x — o padrão é mês curto; o relatório
    de meio de mês passa os nomes de categoria."""
    fmt = rotulo_x or mes_curto
    m = {"e": 40, "d": 8, "t": 12, "b": 26}
    pw, ph = largura - m["e"] - m["d"], altura - m["t"] - m["b"]
    vmax = max(max(s[2]) for s in series) * 1.12 or 1
    def y_de(v): return m["t"] + ph - (v / vmax) * ph
    passo = pw / max(len(meses), 1)
    n = len(series)
    lb = passo * 0.62 / n

    p = [f'<svg viewBox="0 0 {largura} {altura}" role="img">']
    p.append(_eixo_y(m["e"], largura - m["d"], y_de, vmax))
    for i, mes in enumerate(meses):
        base = m["e"] + passo * (i + 0.5) - (lb * n) / 2
        for k, (_, cor, vals) in enumerate(series):
            v = max(vals[i], 0)
            y = y_de(v)
            p.append(f'<rect class="fatia" x="{base+k*lb:.1f}" y="{y:.1f}" '
                     f'width="{lb:.1f}" height="{max(y_de(0)-y,0.6):.1f}" '
                     f'fill="{cor}" rx="2"/>')
        p.append(f'<text class="eixo" x="{m["e"]+passo*(i+0.5):.1f}" y="{altura-13}" '
                 f'text-anchor="middle">{esc(fmt(mes))}</text>')
    p.append(f'<line class="linha-base" x1="{m["e"]}" y1="{y_de(0):.1f}" '
             f'x2="{largura-m["d"]}" y2="{y_de(0):.1f}"/>')
    p.append("</svg>")
    return "".join(p)


def linhas(meses, series, largura=640, altura=200, formato="k"):
    """series: lista de (rotulo, cor, [valores]) — rotuladas direto na ponta."""
    m = {"e": 40, "d": 78, "t": 12, "b": 26}  # margem direita cabe o rótulo direto
    pw, ph = largura - m["e"] - m["d"], altura - m["t"] - m["b"]
    todos = [v for _, _, vs in series for v in vs if v is not None]
    vmin, vmax = min(todos), max(todos)
    faixa = (vmax - vmin) or 1
    lo, hi = vmin - faixa * 0.12, vmax + faixa * 0.12
    def y_de(v): return m["t"] + ph - ((v - lo) / (hi - lo)) * ph
    def x_de(i): return m["e"] + (pw * i / max(len(meses) - 1, 1))

    p = [f'<svg viewBox="0 0 {largura} {altura}" role="img">']
    for i in range(5):
        v = lo + (hi - lo) * i / 4
        y = y_de(v)
        rot = f"{v/1000:.0f}k" if formato == "k" else f"{v:.2f}".replace(".", ",")
        p.append(f'<line class="grade" x1="{m["e"]}" y1="{y:.1f}" '
                 f'x2="{largura-m["d"]}" y2="{y:.1f}"/>')
        p.append(f'<text class="eixo" x="{m["e"]-4}" y="{y+3:.1f}" text-anchor="end">{rot}</text>')
    for rotulo, cor, vals in series:
        d = " ".join(f'{"M" if i == 0 else "L"}{x_de(i):.1f},{y_de(v):.1f}'
                     for i, v in enumerate(vals))
        p.append(f'<path class="serie-linha" d="{d}" stroke="{cor}"/>')
        xf, yf = x_de(len(vals) - 1), y_de(vals[-1])
        p.append(f'<circle cx="{xf:.1f}" cy="{yf:.1f}" r="3.2" fill="{cor}" '
                 f'stroke="#ffffff" stroke-width="2"/>')
        p.append(f'<text class="rotulo-direto" x="{xf+7:.1f}" y="{yf+3:.1f}">{esc(rotulo)}</text>')
    for i, mes in enumerate(meses):
        if len(meses) > 8 and i % 2 == 1 and i != len(meses) - 1:
            continue
        p.append(f'<text class="eixo" x="{x_de(i):.1f}" y="{altura-13}" '
                 f'text-anchor="middle">{mes_curto(mes)}</text>')
    p.append("</svg>")
    return "".join(p)


def barras_alvo(linhas_dados, largura=640, altura=None):
    """Atual vs alvo por camada. linhas_dados: (rotulo, cor, pct_atual, pct_alvo)."""
    lh = 30
    altura = altura or (len(linhas_dados) * lh + 26)
    m = {"e": 96, "d": 78}
    pw = largura - m["e"] - m["d"]
    escala = max([max(a, b) for _, _, a, b in linhas_dados] + [10]) * 1.15

    p = [f'<svg viewBox="0 0 {largura} {altura}" role="img">']
    for i, (rotulo, cor, atual, alvo) in enumerate(linhas_dados):
        y = 10 + i * lh
        wa, wt = pw * atual / escala, pw * alvo / escala
        p.append(f'<text class="eixo" x="{m["e"]-8}" y="{y+13}" text-anchor="end" '
                 f'style="font-size:8.5px;font-weight:600">{esc(rotulo)}</text>')
        p.append(f'<rect x="{m["e"]}" y="{y+3}" width="{max(wa,1):.1f}" height="15" '
                 f'fill="{cor}" rx="3"/>')
        # marcador do alvo: linha vertical + rótulo, nunca cor sozinha
        p.append(f'<line x1="{m["e"]+wt:.1f}" y1="{y-1}" x2="{m["e"]+wt:.1f}" '
                 f'y2="{y+22}" stroke="#0b0b0b" stroke-width="1.6" '
                 f'stroke-dasharray="3 2"/>')
        desvio = atual - alvo
        p.append(f'<text class="rotulo-direto" x="{largura-m["d"]+6}" y="{y+14}">'
                 f'{atual:.0f}% <tspan fill="#52514e" style="font-weight:400">'
                 f'({desvio:+.0f} p.p.)</tspan></text>')
    p.append(f'<text class="eixo" x="{m["e"]}" y="{altura-3}" '
             f'style="font-size:7.5px">Barra: alocação atual · Tracejado: alvo da política</text>')
    p.append("</svg>")
    return "".join(p)


def legenda(itens):
    sp = "".join(f'<span><i class="chip" style="background:{c}"></i>{esc(r)}</span>'
                 for r, c in itens)
    return f'<div class="legenda">{sp}</div>'


def linhas_dia(dias, series, faixa=None, largura=640, altura=210):
    """Acumulado ao longo dos dias do mês.

    Diferente de `linhas()` em três pontos que importam:

    - o eixo x é dia do ciclo de fatura (1..N), não mês;
    - uma série pode ser **mais curta** que o eixo. O mês corrente para no dia
      do corte, e a linha para junto. Completar com zero até o fim do mês faria
      o gasto parecer despencar;
    - `faixa` desenha a envoltória mín–máx dos meses fechados como referência,
      não como entidade: preenchimento neutro atrás de tudo, sem cor de série.
      Cor categórica aqui diria ao leitor que a faixa é mais uma carteira.

    series: lista de (rotulo, cor, [valores], tracejada)
    faixa:  (minimos, maximos) — mesmo comprimento de `dias`
    """
    m = {"e": 40, "d": 92, "t": 12, "b": 26}
    pw, ph = largura - m["e"] - m["d"], altura - m["t"] - m["b"]
    todos = [v for _, _, vs, *_ in series for v in vs if v is not None]
    if faixa:
        todos += [v for v in faixa[1] if v is not None]
    vmax = (max(todos) if todos else 1) * 1.10 or 1
    def y_de(v): return m["t"] + ph - (v / vmax) * ph
    def x_de(i): return m["e"] + (pw * i / max(len(dias) - 1, 1))

    p = [f'<svg viewBox="0 0 {largura} {altura}" role="img">']

    # envoltória primeiro, para ficar atrás das linhas
    if faixa:
        lo, hi = faixa
        ida = " ".join(f'{"M" if i == 0 else "L"}{x_de(i):.1f},{y_de(v):.1f}'
                       for i, v in enumerate(hi))
        volta = " ".join(f'L{x_de(i):.1f},{y_de(v):.1f}'
                         for i in range(len(lo) - 1, -1, -1) for v in [lo[i]])
        p.append(f'<path class="faixa-ref" d="{ida} {volta} Z"/>')

    p.append(_eixo_y(m["e"], largura - m["d"], y_de, vmax))

    for s in series:
        rotulo, cor, vals, tracejada = (list(s) + [False])[:4]
        d = " ".join(f'{"M" if i == 0 else "L"}{x_de(i):.1f},{y_de(v):.1f}'
                     for i, v in enumerate(vals))
        traco = ' stroke-dasharray="5 3"' if tracejada else ""
        p.append(f'<path class="serie-linha" d="{d}" stroke="{cor}"{traco}/>')
        xf, yf = x_de(len(vals) - 1), y_de(vals[-1])
        p.append(f'<circle cx="{xf:.1f}" cy="{yf:.1f}" r="3.2" fill="{cor}" '
                 f'stroke="#ffffff" stroke-width="2"/>')
        p.append(f'<text class="rotulo-direto" x="{xf+7:.1f}" y="{yf+3:.1f}" '
                 f'fill="{cor}">{esc(rotulo)}</text>')

    # marcas de dia: só as que cabem sem colidir
    for i, dia in enumerate(dias):
        if dia % 5 and dia != 1 and i != len(dias) - 1:
            continue
        p.append(f'<text class="eixo" x="{x_de(i):.1f}" y="{altura-13}" '
                 f'text-anchor="middle">{dia}</text>')
    p.append(f'<line class="linha-base" x1="{m["e"]}" y1="{y_de(0):.1f}" '
             f'x2="{largura-m["d"]}" y2="{y_de(0):.1f}"/>')
    p.append("</svg>")
    return "".join(p)


def barras_desvio(itens, largura=640, altura=None):
    """Desvio da projeção contra a mediana, por categoria — barra divergente.

    Zero no meio, barra para a direita quando a projeção estoura a mediana e
    para a esquerda quando fica abaixo. Duas cores de polaridade (status), não
    de identidade: aqui a cor significa estado, e vem acompanhada de rótulo com
    o valor, nunca sozinha.

    itens: (rotulo, valor_pp, texto_direito)
    """
    lh = 26
    altura = altura or (len(itens) * lh + 22)
    m = {"e": 96, "d": 108}
    pw = largura - m["e"] - m["d"]
    escala = max([abs(v) for _, v, _ in itens] + [10]) * 1.15
    meio = m["e"] + pw / 2

    p = [f'<svg viewBox="0 0 {largura} {altura}" role="img">']
    for i, (rotulo, valor, txt) in enumerate(itens):
        y = 8 + i * lh
        w = abs(valor) / escala * (pw / 2)
        x = meio if valor >= 0 else meio - w
        cor = "#d03b3b" if valor > 0 else "#0ca30c"
        p.append(f'<text class="eixo" x="{m["e"]-8}" y="{y+13}" text-anchor="end" '
                 f'style="font-size:8.5px;font-weight:600">{esc(rotulo)}</text>')
        p.append(f'<rect x="{x:.1f}" y="{y+3}" width="{max(w,1):.1f}" height="13" '
                 f'fill="{cor}" rx="3"/>')
        p.append(f'<text class="rotulo-direto" x="{largura-m["d"]+6}" y="{y+13}">'
                 f'{esc(txt)}</text>')
    p.append(f'<line x1="{meio:.1f}" y1="4" x2="{meio:.1f}" y2="{altura-16}" '
             f'stroke="#0b0b0b" stroke-width="1.2"/>')
    p.append(f'<text class="eixo" x="{m["e"]}" y="{altura-3}" '
             f'style="font-size:7.5px">Direita: projeção acima da mediana de 6 meses '
             f'· Esquerda: abaixo</text>')
    p.append("</svg>")
    return "".join(p)

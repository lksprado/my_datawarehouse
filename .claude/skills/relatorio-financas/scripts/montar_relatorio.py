#!/usr/bin/env python3
"""Monta o HTML de um relatório financeiro a partir do JSON extraído.

    montar_relatorio.py --dados D.json --escopo casal|deusa \
                        --narrativa N.json --saida R.html

Divisão de responsabilidades: este script renderiza tudo que é *calculável*
— KPIs, tabelas, gráficos SVG — direto dos dados, para que nenhum número do
PDF dependa de transcrição manual. O arquivo de narrativa traz apenas o texto
analítico (diagnóstico e recomendações), que é escrito pelo agente.

Estrutura do arquivo de narrativa (todas as chaves opcionais):

    {
      "sumario": "<p>…</p>",
      "diagnostico_orcamento": "<p>…</p>",
      "diagnostico_carteira": "<p>…</p>",
      "diagnostico_riscos": "<p>…</p>",
      "diagnostico_desempenho": "<p>…</p>",
      "recomendacoes": [{"titulo": "…", "texto": "…"}],
      "premissas": ["…"]
    }
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
CSS = AQUI.parent / "assets" / "relatorio.css"

MESES = ["", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
         "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
MES_CURTO = ["", "jan", "fev", "mar", "abr", "mai", "jun",
             "jul", "ago", "set", "out", "nov", "dez"]

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
          "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
COR_CAMADA = {"RESERVA": SERIES[0], "CRESCIMENTO": SERIES[1], "RENDA": SERIES[2],
              "NAO CLASSIFICADO": "#7a7975"}
CATEGORIAS = ["mercado", "diversos", "assinaturas", "role",
              "transporte", "apartamento", "saude", "educacao"]
ROTULO_CAT = {"mercado": "Mercado", "diversos": "Diversos",
              "assinaturas": "Assinaturas", "role": "Rolê",
              "transporte": "Transporte", "apartamento": "Apartamento",
              "saude": "Saúde", "educacao": "Educação"}
COR_CAT = {c: SERIES[i] for i, c in enumerate(CATEGORIAS)}

TEXTO_CAMADA = {
    "RESERVA": "Liquidez e preservação de capital. Resgate imediato, sem "
               "marcação a mercado negativa. Dimensionada em meses de despesa.",
    "CRESCIMENTO": "Acumulação de patrimônio no longo prazo, aceitando "
                   "volatilidade. Horizonte de cinco anos ou mais.",
    "RENDA": "Geração de fluxo de caixa recorrente e previsível. O que "
             "qualifica é a regularidade do pagamento, não o retorno total.",
    "NAO CLASSIFICADO": "Ativo ainda sem camada atribuída na planilha. Não é "
                        "uma alocação — é pendência de classificação.",
}
TEXTO_CATEGORIA = {
    "mercado": "Supermercado, hortifruti, açougue, padaria, bebidas de casa, "
               "limpeza e higiene. Não inclui refeição fora nem farmácia.",
    "diversos": "Categoria residual: vestuário, presentes, eletrônicos, taxas "
                "bancárias, imprevistos. O que não cabe nas demais.",
    "assinaturas": "Serviços recorrentes de cobrança automática: streaming, "
                   "nuvem, software, telefonia, academia.",
    "role": "Lazer e consumo fora de casa: bares, restaurantes, delivery, "
            "cinema, eventos, viagens.",
    "transporte": "Combustível, aplicativos, transporte público, "
                  "estacionamento, pedágio, manutenção, seguro e IPVA.",
    "apartamento": "Moradia: aluguel ou financiamento, condomínio, IPTU, luz, "
                   "água, gás, internet fixa, móveis e reformas.",
    "saude": "Plano de saúde, consultas, exames, odontologia, terapia e "
             "farmácia.",
    "educacao": "Cursos, graduação, certificações, livros técnicos e material "
                "didático.",
}


# ------------------------------------------------------------- formatação ---

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
    return f"{'+' if v >= 0 else ''}{float(v):.{casas}f}{sufixo}".replace(".", ",")


def data_br(s):
    if not s:
        return "—"
    d = datetime.strptime(s[:10], "%Y-%m-%d").date()
    return d.strftime("%d/%m/%Y")


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


# ---------------------------------------------------------------- gráficos ---
# SVG inline, sem biblioteca. Todo gráfico tem tabela irmã na mesma seção —
# a paleta tem slots abaixo de 3:1 na superfície clara e a tabela é a
# mitigação exigida pela regra de alívio da skill dataviz.

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


def barras_agrupadas(meses, series, largura=640, altura=200):
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
                 f'text-anchor="middle">{mes_curto(mes)}</text>')
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


# ------------------------------------------------------------------ blocos ---

def kpi(rotulo, valor, nota="", cls=""):
    n = f'<div class="nota {cls}">{nota}</div>' if nota else ""
    return f'<div class="kpi"><div class="rotulo">{esc(rotulo)}</div>' \
           f'<div class="valor">{valor}</div>{n}</div>'


def tabela(cabecalhos, linhas_, rodape=None, alinhamentos=None):
    al = alinhamentos or ["l"] + ["n"] * (len(cabecalhos) - 1)
    th = "".join(f'<th class="{"num" if a == "n" else ""}">{esc(h)}</th>'
                 for h, a in zip(cabecalhos, al))
    corpo = []
    for ln in linhas_:
        cls = ' class="destaque"' if isinstance(ln, dict) else ""
        cels = ln["cels"] if isinstance(ln, dict) else ln
        tds = "".join(f'<td class="{"num" if a == "n" else ""}">{c}</td>'
                      for c, a in zip(cels, al))
        corpo.append(f"<tr{cls}>{tds}</tr>")
    tf = ""
    if rodape:
        tds = "".join(f'<td class="{"num" if a == "n" else ""}">{c}</td>'
                      for c, a in zip(rodape, al))
        tf = f"<tfoot><tr>{tds}</tr></tfoot>"
    return (f'<table><thead><tr>{th}</tr></thead>'
            f'<tbody>{"".join(corpo)}</tbody>{tf}</table>')


def secao(num, titulo, sub, conteudo, quebra=False):
    s = f'<div class="sub">{esc(sub)}</div>' if sub else ""
    q = " quebra" if quebra else ""
    return (f'<section class="{q.strip()}"><h2><span class="num">{num}</span>'
            f'{esc(titulo)}</h2>{s}{conteudo}</section>')


def bloco(titulo, html, tipo=""):
    t = f"<h4>{esc(titulo)}</h4>" if titulo else ""
    return f'<div class="destaque-bloco {tipo}">{t}{html}</div>'


def tag_risco(txt):
    mapa = {"OK": "tag-ok", "PERIGO!": "tag-critico"}
    icone = {"OK": "✓", "PERIGO!": "▲"}
    return (f'<span class="tag {mapa.get(txt, "tag-atencao")}">'
            f'{icone.get(txt, "•")} {esc(txt)}</span>')


# ------------------------------------------------------------------ cálculo ---

def media(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def variacao(atual, base):
    return None if not base else (atual - base) / base * 100


def alvos_camada(escopo):
    """Alocação-alvo de _docs_financas.md. [CONFIRMAR]"""
    return {
        "lucas":   {"RESERVA": 25, "CRESCIMENTO": 55, "RENDA": 20},
        "jessica": {"RESERVA": 40, "CRESCIMENTO": 50, "RENDA": 10},
        "deusa":   {"RESERVA": 40, "CRESCIMENTO": 25, "RENDA": 35},
    }


APORTE_ALVO = {"lucas": 4000, "jessica": 2500, "deusa": 1000}
META_RESERVA_MESES = {"casal": 6, "deusa": 12}
META_POUPANCA_PCT = 35
FGC_FOLGA_MINIMA = 50000


# ----------------------------------------------------------------- carteira ---

def secao_carteira(d, pessoa, num_ref):
    camadas = [c for c in d["carteira_camada"] if c["pessoa"] == pessoa]
    total = sum(c["valor"] for c in camadas)
    if not total:
        return ""
    alvos = alvos_camada(None)[pessoa]
    atual = {c["camada"]: float(c["pct_da_carteira"]) for c in camadas}
    valores = {c["camada"]: c["valor"] for c in camadas}

    dados_alvo = [(cm, COR_CAMADA[cm], atual.get(cm, 0.0), alvos.get(cm, 0))
                  for cm in ["RESERVA", "CRESCIMENTO", "RENDA"]]
    nc = atual.get("NAO CLASSIFICADO")

    lin = []
    for cm, cor, a, alvo in dados_alvo:
        desvio = a - alvo
        dentro = abs(desvio) <= 5
        lin.append([
            f'<i class="chip" style="background:{cor}"></i>{cm.capitalize()}',
            brl(valores.get(cm, 0)), pct(a), f"{alvo}%",
            # desvio do alvo é ruim nos dois sentidos — nunca verde por ser positivo
            f'<span class="{"" if dentro else "delta-neg"}">'
            f'{sinal(desvio, 0, " p.p.")}</span>',
            '<span class="tag tag-ok">✓ na banda</span>' if dentro else
            '<span class="tag tag-atencao">▲ fora da banda</span>',
        ])
    if nc:
        lin.append([
            f'<i class="chip" style="background:{COR_CAMADA["NAO CLASSIFICADO"]}"></i>Não classificado',
            brl(valores.get("NAO CLASSIFICADO", 0)), pct(nc), "—", "—",
            '<span class="tag tag-atencao">▲ classificar</span>'])

    inst = [i for i in d["carteira_instituicao"] if i["pessoa"] == pessoa]
    tipos = [t for t in d["carteira_tipo"] if t["pessoa"] == pessoa]
    moedas = {m["moeda_ativo"]: float(m["pct_da_carteira"])
              for m in d["carteira_moeda"] if m["pessoa"] == pessoa}
    posicoes = [p for p in d["posicoes"] if p["pessoa"] == pessoa]

    html = [
        f'<div class="kpis tres">',
        kpi("Carteira total", brl(total), f"{len(posicoes)} posições"),
        kpi("Maior instituição", inst[0]["instituicao"].title() if inst else "—",
            f'{pct(inst[0]["pct_da_carteira"])} da carteira' if inst else ""),
        kpi("Exposição em dólar", pct(moedas.get("USD", 0)), "alvo 15% [CONFIRMAR]"),
        "</div>",
        "<h3>Alocação por camada contra a política</h3>",
        f"<figure>{barras_alvo(dados_alvo)}</figure>",
        tabela(["Camada", "Valor", "Atual", "Alvo", "Desvio", "Situação"], lin),
        "<h3>Concentração</h3>",
        tabela(["Instituição", "Valor", "% da carteira"],
               [[i["instituicao"].title(), brl(i["valor"]), pct(i["pct_da_carteira"])]
                for i in inst],
               ["Total", brl(total), "100,0%"]),
        tabela(["Categoria", "Tipo", "Valor", "% da carteira"],
               [[t["categoria_investimento"].title(), t["tipo_investimento"].title(),
                 brl(t["valor"]), pct(t["pct_da_carteira"])] for t in tipos],
               None, ["l", "l", "n", "n"]),
        "<h3>Posições</h3>",
        tabela(["Investimento", "Camada", "Instituição", "Indexador", "Vencimento", "Valor"],
               [[esc(p["investimento"]), p["camada"].capitalize(),
                 p["instituicao"].title(), p["indexador"] or "—",
                 data_br(p["data_vencimento"]) if p["data_vencimento"] else "—",
                 brl(p["vlr_atualizado_brl"])] for p in posicoes],
               ["Total", "", "", "", "", brl(total)],
               ["l", "l", "l", "l", "l", "n"]),
    ]
    return "".join(html)


# ------------------------------------------------------------------ montagem ---

def cabecalho(titulo, subtitulo, meta, destinatarios):
    css = CSS.read_text(encoding="utf-8")
    defasagem = meta["defasagem_carteira_meses"]
    aviso = ""
    if defasagem:
        aviso = (f"<p style='margin-top:4mm'><strong>Atenção às datas.</strong> "
                 f"Orçamento apurado em {mes_extenso(meta['mes_ref'])}; carteira e "
                 f"patrimônio posicionados em {mes_extenso(meta['mes_carteira'])} "
                 f"— o fechamento da carteira está {defasagem} mês à frente de ser "
                 f"conciliado.</p>")
    return f"""<meta charset="utf-8"><title>{esc(titulo)}</title><style>{css}</style>
<div class="capa">
  <div class="eyebrow">Relatório de planejamento financeiro e investimentos</div>
  <h1>{esc(titulo)}</h1>
  <div class="periodo">{esc(subtitulo)}</div>
  <p style="max-width:120mm;color:var(--text-secondary)">
    Preparado para {esc(destinatarios)}. Reúne a apuração do orçamento, a
    posição consolidada da carteira, a leitura de riscos e o destino recomendado
    para o aporte do período.{aviso}
  </p>
  <div class="rodape-capa">
    Gerado em {data_br(meta['gerado_em'])} a partir da camada <code>marts</code>
    do data warehouse · mês de referência {meta['mes_ref'][:7]}
  </div>
</div>"""


def rodape(meta, premissas):
    pr = ""
    if premissas:
        itens = "".join(f"<li>{esc(p)}</li>" for p in premissas)
        pr = (f"<p><strong>Premissas marcadas [CONFIRMAR] usadas neste "
              f"relatório:</strong></p><ul>{itens}</ul>")
    return f"""<section class="quebra"><h2><span class="num">—</span>Notas e procedência</h2>
<div class="rodape-doc">
  {pr}
  <p>Origem: camada <code>marts</code> do data warehouse pessoal (PostgreSQL),
  domínio finanças. Orçamento e consumo em {mes_extenso(meta['mes_ref'])};
  carteira, patrimônio e renda passiva em {mes_extenso(meta['mes_carteira'])}.</p>
  <p>Definições de categoria de gasto e de camada de investimento conforme
  <code>models/marts/financas/_docs_financas.md</code>, fonte única do domínio.
  Meses posteriores ao de referência existem na base como lançamentos futuros
  pré-agendados e foram excluídos de todos os números.</p>
  <p>Este documento é gerado automaticamente a partir de dados próprios e não
  constitui recomendação de investimento de profissional certificado.</p>
  <p>Gerado em {data_br(meta['gerado_em'])}.</p>
</div></section>"""


def glossario(escopo):
    itens = []
    if escopo == "casal":
        itens.append("<h3>Categorias de gasto</h3><dl class='glossario'>")
        for c in CATEGORIAS:
            itens.append(f'<dt><i class="chip" style="background:{COR_CAT[c]}"></i>'
                         f'{ROTULO_CAT[c]}</dt><dd>{TEXTO_CATEGORIA[c]}</dd>')
        itens.append("</dl>")
    itens.append("<h3>Camadas de investimento</h3><dl class='glossario'>")
    for cm in ["RESERVA", "CRESCIMENTO", "RENDA", "NAO CLASSIFICADO"]:
        itens.append(f'<dt><i class="chip" style="background:{COR_CAMADA[cm]}"></i>'
                     f'{cm.capitalize()}</dt><dd>{TEXTO_CAMADA[cm]}</dd>')
    itens.append("</dl>")
    itens.append(
        "<p class='sub'>A camada descreve o papel do ativo na carteira, não o que "
        "ele é. Um CDB pode ser reserva ou crescimento conforme sua liquidez e "
        "exposição a marcação a mercado.</p>")
    return "".join(itens)


def recomendacoes_html(recs):
    if not recs:
        return "<p>Sem recomendações para o período.</p>"
    li = "".join(f'<li><span class="titulo-rec">{esc(r["titulo"])}</span>{r["texto"]}</li>'
                 for r in recs)
    return f'<ol class="recomendacoes">{li}</ol>'


def montar_casal(d, n):
    meta = d["meta"]
    dre = d["dre_mensal"]
    ult = dre[-1]
    meses = [r["mes_debito"] for r in dre]
    pat = d["patrimonio"]
    upat = pat[-1]

    desp6 = media([r["total_despesas"] for r in dre[-7:-1]])
    poupanca = ult["resultado"] / ult["total_receita"] * 100 if ult["total_receita"] else 0
    poupanca12 = (sum(r["resultado"] for r in dre) /
                  sum(r["total_receita"] for r in dre) * 100)
    var_pat = variacao(upat["total_patrimonio_liquido"],
                       pat[-2]["total_patrimonio_liquido"]) if len(pat) > 1 else None

    reserva_casal = sum(c["valor"] for c in d["carteira_camada"]
                        if c["pessoa"] in ("lucas", "jessica") and c["camada"] == "RESERVA")
    cobertura = reserva_casal / desp6 if desp6 else 0

    partes = [cabecalho(
        "Relatório financeiro do casal",
        f"{mes_extenso(meta['mes_ref']).capitalize()}",
        meta, "Lucas e Jéssica")]

    # 1 — sumário
    partes.append(secao(1, "Sumário executivo",
        f"Apuração de {mes_extenso(meta['mes_ref'])}",
        '<div class="kpis">'
        + kpi("Patrimônio líquido", brl(upat["total_patrimonio_liquido"]),
              f"{sinal(var_pat)} vs. mês anterior", classe_delta(var_pat))
        + kpi("Resultado do mês", brl(ult["resultado"]),
              f"{pct(poupanca)} da receita")
        + kpi("Despesa total", brl(ult["total_despesas"]),
              f'{sinal(variacao(ult["total_despesas"], desp6))} vs. média 6m',
              classe_delta(variacao(ult["total_despesas"], desp6), bom_se_sobe=False))
        + kpi("Reserva de emergência", f"{cobertura:.1f}".replace(".", ",") + " meses",
              f'meta {META_RESERVA_MESES["casal"]} meses',
              "delta-pos" if cobertura >= 6 else "delta-neg")
        + "</div>"
        + (n.get("sumario") or "")))

    # 2 — patrimônio
    partes.append(secao(2, "Patrimônio",
        f"Posição em {mes_extenso(meta['mes_carteira'])} · últimos 12 meses",
        f'<figure>{barras_empilhadas([r["mes_base"] for r in pat], [("Lucas", SERIES[0], [r["patrimonio_liquido_lucas"] for r in pat]), ("Jéssica", SERIES[1], [r["patrimonio_liquido_jessica"] for r in pat])])}'
        f'{legenda([("Lucas", SERIES[0]), ("Jéssica", SERIES[1])])}'
        f'<figcaption>Patrimônio líquido por titular, em reais. Rótulo no topo = total do casal.</figcaption></figure>'
        + tabela(["Mês", "Bruto", "Líquido", "Lucas", "Jéssica", "Var. líq."],
                 [[mes_curto(r["mes_base"]), brl(r["total_patrimonio_bruto"]),
                   brl(r["total_patrimonio_liquido"]), brl(r["patrimonio_liquido_lucas"]),
                   brl(r["patrimonio_liquido_jessica"]),
                   f'<span class="{classe_delta(variacao(r["total_patrimonio_liquido"], pat[i-1]["total_patrimonio_liquido"]) if i else None)}">'
                   f'{sinal(variacao(r["total_patrimonio_liquido"], pat[i-1]["total_patrimonio_liquido"]) if i else None)}</span>']
                  for i, r in enumerate(pat)])))

    # 3 — resultado
    partes.append(secao(3, "Resultado do mês",
        "Receita, despesa e taxa de poupança · últimos 13 meses",
        f'<figure>{barras_agrupadas(meses, [("Receita", SERIES[0], [r["total_receita"] for r in dre]), ("Despesa", SERIES[1], [r["total_despesas"] for r in dre])])}'
        f'{legenda([("Receita", SERIES[0]), ("Despesa", SERIES[1])])}'
        f'<figcaption>Receita e despesa mensais, em reais.</figcaption></figure>'
        + tabela(["Mês", "Receita", "Despesa", "Resultado", "Poupança"],
                 [[mes_curto(r["mes_debito"]), brl(r["total_receita"]),
                   brl(r["total_despesas"]),
                   f'<span class="{classe_delta(r["resultado"])}">{brl(r["resultado"])}</span>',
                   pct(r["resultado"] / r["total_receita"] * 100 if r["total_receita"] else 0)]
                  for r in dre],
                 ["Média 13m", brl(media([r["total_receita"] for r in dre])),
                  brl(media([r["total_despesas"] for r in dre])),
                  brl(media([r["resultado"] for r in dre])), pct(poupanca12)])
        + bloco("Leitura do orçamento", n.get("diagnostico_orcamento", ""))))

    # 4 — gastos por categoria
    series_cat = [(ROTULO_CAT[c], COR_CAT[c], [r[f"total_{c}"] for r in dre])
                  for c in CATEGORIAS]
    lin_cat = []
    for c in CATEGORIAS:
        atual = ult[f"total_{c}"]
        m6 = media([r[f"total_{c}"] for r in dre[-7:-1]])
        part = atual / ult["total_despesas"] * 100 if ult["total_despesas"] else 0
        v = variacao(atual, m6)
        lin_cat.append([
            f'<i class="chip" style="background:{COR_CAT[c]}"></i>{ROTULO_CAT[c]}',
            brl(atual), pct(part), brl(m6),
            f'<span class="{classe_delta(v, bom_se_sobe=False)}">{sinal(v)}</span>'])
    partes.append(secao(4, "Gastos por categoria",
        f"{mes_extenso(meta['mes_ref']).capitalize()} contra a média dos 6 meses anteriores",
        f'<figure>{barras_empilhadas(meses, series_cat, altura=230)}'
        f'{legenda([(ROTULO_CAT[c], COR_CAT[c]) for c in CATEGORIAS])}'
        f'<figcaption>Composição da despesa mensal por categoria, em reais.</figcaption></figure>'
        + tabela(["Categoria", "No mês", "% da despesa", "Média 6m", "Var. vs 6m"], lin_cat,
                 ["Total", brl(ult["total_despesas"]), "100,0%", brl(desp6),
                  sinal(variacao(ult["total_despesas"], desp6))])
        + "<h3>Conta de energia</h3>"
        + tabela(["Mês", "Fatura", "kWh", "kWh/dia", "Preço kWh"],
                 [[mes_curto(r["mes"]), brl(r["vlr_fatura"], 2), f'{r["kwh"]}',
                   f'{r["kwh_dia"]}'.replace(".", ","),
                   f'R$ {r["preco_kwh"]}'.replace(".", ",")] for r in d["luz"][-6:]])
        + '<p class="sub">Separar efeito preço de efeito consumo: variação em '
          'kWh/dia é comportamento; variação em preço por kWh é tarifa.</p>',
        quebra=True))

    # 5 — carteira
    for i, p in enumerate(["lucas", "jessica"]):
        partes.append(secao("5" + ".ab"[i], f"Carteira — {p.capitalize()}",
            f"Posição em {mes_extenso(meta['mes_carteira'])}",
            secao_carteira(d, p, 5), quebra=True))
    partes.append(secao("5.c", "Leitura da carteira", "",
        bloco("", n.get("diagnostico_carteira", ""))))

    # 6 — renda passiva
    div = [r for r in d["dividendos"] if r["pessoa"] in ("lucas", "jessica")]
    mdiv = sorted({r["mes_base"] for r in div})
    sl = [sum(r["vlr_liquido_brl"] for r in div
              if r["mes_base"] == m and r["pessoa"] == "lucas") for m in mdiv]
    sj = [sum(r["vlr_liquido_brl"] for r in div
              if r["mes_base"] == m and r["pessoa"] == "jessica") for m in mdiv]
    tot12 = sum(sl) + sum(sj)
    cart_casal = sum(c["valor"] for c in d["carteira_camada"]
                     if c["pessoa"] in ("lucas", "jessica"))
    partes.append(secao(6, "Renda passiva",
        "Dividendos e proventos líquidos · últimos 12 meses",
        '<div class="kpis tres">'
        + kpi("Recebido em 12 meses", brl(tot12))
        + kpi("Média mensal", brl(tot12 / max(len(mdiv), 1)))
        + kpi("Yield sobre a carteira", pct(tot12 / cart_casal * 100 if cart_casal else 0),
              "12 meses sobre a posição atual")
        + "</div>"
        + f'<figure>{barras_empilhadas(mdiv, [("Lucas", SERIES[0], sl), ("Jéssica", SERIES[1], sj)], altura=180)}'
        + legenda([("Lucas", SERIES[0]), ("Jéssica", SERIES[1])])
        + "<figcaption>Proventos líquidos recebidos por mês, em reais.</figcaption></figure>"
        + tabela(["Mês", "Lucas", "Jéssica", "Total"],
                 [[mes_curto(m), brl(sl[i]), brl(sj[i]), brl(sl[i] + sj[i])]
                  for i, m in enumerate(mdiv)],
                 ["Total", brl(sum(sl)), brl(sum(sj)), brl(tot12)])))

    # 7 — riscos
    partes.append(secao(7, "Riscos", "FGC, concentração e vencimentos",
        bloco_riscos(d, ["lucas", "jessica"]) + bloco("", n.get("diagnostico_riscos", "")),
        quebra=True))

    # 8 — desempenho
    partes.append(secao(8, "Desempenho contra benchmarks",
        "Patrimônio líquido acumulado, base 1 em novembro de 2023",
        bloco_desempenho(d) + bloco("", n.get("diagnostico_desempenho", ""))))

    # 9 — recomendações
    partes.append(secao(9, "Recomendações",
        f"Destino sugerido para o aporte · aporte-alvo conjunto "
        f"{brl(APORTE_ALVO['lucas'] + APORTE_ALVO['jessica'])}/mês [CONFIRMAR]",
        recomendacoes_html(n.get("recomendacoes", [])), quebra=True))

    # 10 — glossário
    partes.append(secao(10, "Glossário",
        "O que cada categoria de gasto engloba e o que significa cada camada",
        glossario("casal"), quebra=True))

    partes.append(rodape(meta, n.get("premissas", [])))
    return "\n".join(partes)


def bloco_riscos(d, pessoas):
    fgc = [f for f in d["fgc"] if f["pessoa"] in pessoas]
    venc = [v for v in d["vencimentos_12m"] if v["pessoa"] in pessoas]
    nc = [x for x in d["nao_classificados"] if x["pessoa"] in pessoas]
    h = ["<h3>Exposição ao FGC por conglomerado</h3>",
         '<p class="sub">Garantia de R$ 250.000 por CPF e por conglomerado. '
         'A política exige folga mínima de R$ 50.000.</p>',
         tabela(["Titular", "Conglomerado", "Valor coberto restante", "Situação"],
                [[f["pessoa"].capitalize(), f["conglomerado"].title(),
                  brl(f["vlr_liberado"]), tag_risco(f["risco_fgc"])] for f in fgc],
                None, ["l", "l", "n", "l"])]
    h.append("<h3>Vencimentos nos próximos 12 meses</h3>")
    if venc:
        h.append(tabela(["Titular", "Investimento", "Camada", "Vencimento", "Em", "Valor"],
                        [[v["pessoa"].capitalize(), esc(v["investimento"]),
                          v["camada"].capitalize(), data_br(v["data_vencimento"]),
                          f'{v["vencimento_em_dias"]} d', brl(v["vlr_atualizado_brl"])]
                         for v in venc],
                        ["Total", "", "", "", "",
                         brl(sum(v["vlr_atualizado_brl"] for v in venc))],
                        ["l", "l", "l", "l", "n", "n"]))
    else:
        h.append("<p>Nenhum título vencendo nos próximos 12 meses.</p>")
    h.append("<h3>Ativos sem camada atribuída</h3>")
    if nc:
        h.append(tabela(["Titular", "Investimento", "Instituição", "Valor"],
                        [[x["pessoa"].capitalize(), esc(x["investimento"]),
                          x["instituicao"].title(), brl(x["vlr_atualizado_brl"])]
                         for x in nc]))
        h.append('<p class="sub">Classificar na aba <code>classificacao</code> '
                 'da planilha — esses valores ficam fora dos percentuais por camada.</p>')
    else:
        h.append("<p>Nenhum. Todos os ativos estão classificados por camada.</p>")
    return "".join(h)


def bloco_desempenho(d):
    r = d["riqueza"]
    if not r:
        return "<p>Sem série de benchmark disponível.</p>"
    meses = [x["mes_base"] for x in r]
    u = r[-1]
    return (
        f'<figure>{linhas(meses, [("Patrimônio", SERIES[0], [float(x["total_patrimonio_liquido_acum"]) for x in r]), ("CDI", SERIES[1], [float(x["cdi_acum"]) for x in r]), ("Infl. pessoal", SERIES[2], [float(x["minha_inflacao_acum"]) for x in r])], formato="idx")}'
        f'<figcaption>Índice acumulado, base 1. Séries indexadas à mesma base — '
        f'eixo único.</figcaption></figure>'
        + tabela(["Mês", "Patrimônio", "CDI", "IPCA", "Inflação pessoal"],
                 [[mes_curto(x["mes_base"]),
                   f'{float(x["total_patrimonio_liquido_acum"]):.3f}'.replace(".", ","),
                   f'{float(x["cdi_acum"]):.3f}'.replace(".", ","),
                   f'{float(x["ipca_acum"]):.3f}'.replace(".", ","),
                   f'{float(x["minha_inflacao_acum"]):.3f}'.replace(".", ",")]
                  for x in r])
        + f'<p class="sub">No mês de referência o patrimônio acumula índice '
          f'{float(u["total_patrimonio_liquido_acum"]):.3f}'.replace(".", ",")
        + f' contra {float(u["cdi_acum"]):.3f}'.replace(".", ",")
        + f' do CDI e {float(u["minha_inflacao_acum"]):.3f}'.replace(".", ",")
        + " da inflação pessoal.</p>")


def montar_deusa(d, n):
    meta = d["meta"]
    camadas = [c for c in d["carteira_camada"] if c["pessoa"] == "deusa"]
    total = sum(c["valor"] for c in camadas)
    div = [r for r in d["dividendos"] if r["pessoa"] == "deusa"]
    mdiv = sorted({r["mes_base"] for r in div})
    sv = [sum(r["vlr_liquido_brl"] for r in div if r["mes_base"] == m) for m in mdiv]
    tot12 = sum(sv)
    reserva = next((c["valor"] for c in camadas if c["camada"] == "RESERVA"), 0)

    partes = [cabecalho("Relatório de investimentos",
                        mes_extenso(meta["mes_carteira"]).capitalize(),
                        meta, "Deusa")]

    partes.append(secao(1, "Sumário executivo",
        f"Posição da carteira em {mes_extenso(meta['mes_carteira'])}",
        '<div class="kpis">'
        + kpi("Carteira total", brl(total))
        + kpi("Reserva", brl(reserva), f"{reserva/total*100:.0f}% da carteira" if total else "")
        + kpi("Renda passiva 12m", brl(tot12), f"média {brl(tot12/max(len(mdiv),1))}/mês")
        + kpi("Yield da carteira", pct(tot12 / total * 100 if total else 0), "12 meses")
        + "</div>"
        + (n.get("sumario") or "")
        + '<p class="sub">Este relatório cobre exclusivamente investimentos. '
          'Não há lançamentos de receita e despesa registrados para Deusa no '
          'data warehouse, portanto não há apuração de orçamento.</p>'))

    partes.append(secao(2, "Carteira", f"Posição em {mes_extenso(meta['mes_carteira'])}",
        secao_carteira(d, "deusa", 2) + bloco("Leitura da carteira",
                                              n.get("diagnostico_carteira", "")),
        quebra=True))

    partes.append(secao(3, "Renda passiva",
        "Dividendos e proventos líquidos · últimos 12 meses",
        f'<figure>{barras_empilhadas(mdiv, [("Proventos", SERIES[2], sv)], altura=180)}'
        f'<figcaption>Proventos líquidos recebidos por mês, em reais.</figcaption></figure>'
        + tabela(["Mês", "Recebido"], [[mes_curto(m), brl(sv[i])] for i, m in enumerate(mdiv)],
                 ["Total 12 meses", brl(tot12)]),
        quebra=True))

    partes.append(secao(4, "Riscos", "FGC, concentração e vencimentos",
        bloco_riscos(d, ["deusa"]) + bloco("", n.get("diagnostico_riscos", ""))))

    partes.append(secao(5, "Recomendações",
        f"Destino sugerido para o aporte · aporte-alvo {brl(APORTE_ALVO['deusa'])}/mês [CONFIRMAR]",
        recomendacoes_html(n.get("recomendacoes", [])), quebra=True))

    partes.append(secao(6, "Glossário", "O que significa cada camada de investimento",
        glossario("deusa")))

    partes.append(rodape(meta, n.get("premissas", [])))
    return "\n".join(partes)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dados", required=True)
    ap.add_argument("--escopo", required=True, choices=["casal", "deusa"])
    ap.add_argument("--narrativa", required=True)
    ap.add_argument("--saida", required=True)
    a = ap.parse_args()

    d = json.loads(Path(a.dados).read_text(encoding="utf-8"))
    n = json.loads(Path(a.narrativa).read_text(encoding="utf-8"))
    html = montar_casal(d, n) if a.escopo == "casal" else montar_deusa(d, n)
    saida = Path(a.saida)
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(html, encoding="utf-8")
    print(f"HTML montado: {saida} ({len(html)//1024} KB)")


if __name__ == "__main__":
    main()

"""Blocos de HTML compartilhados: KPIs, tabelas, seções e glossário.

Capa, rodapé e tarja de prontidão **não** moram aqui: cada relatório tem os
seus, porque o texto de procedência e os portões são diferentes. O que é
comum é a mecânica — a grade de KPI, a tabela com cabeçalho repetido a cada
página, a seção numerada.
"""
from __future__ import annotations

from pathlib import Path

from . import politica
from .formato import esc

CSS = Path(__file__).resolve().parent / "relatorio.css"


def css_inline():
    """O CSS vai embutido em <style>. O Chrome do `html_para_pdf.sh` roda sem
    rede: folha externa sairia em branco."""
    return CSS.read_text(encoding="utf-8")


def rotulo_chip(cor, texto):
    """Chip de cor + rótulo, indivisíveis.

    Sem o `nowrap`, numa tabela estreita o chip fica sozinho na primeira linha
    e a palavra cai para a segunda — a linha dobra de altura e a cor se
    desgruda do nome que ela identifica."""
    return (f'<span class="rotulo-chip">'
            f'<i class="chip" style="background:{cor}"></i>{esc(texto)}</span>')


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


def tag(txt, tipo="tag-ok", icone="✓"):
    """Marcador genérico — situação de banda, de ritmo, de projeção."""
    return f'<span class="tag {tipo}">{icone} {esc(txt)}</span>'


def recomendacoes_html(recs):
    if not recs:
        return "<p>Sem recomendações para o período.</p>"
    li = "".join(f'<li><span class="titulo-rec">{esc(r["titulo"])}</span>{r["texto"]}</li>'
                 for r in recs)
    return f'<ol class="recomendacoes">{li}</ol>'


# ---------------------------------------------------------------- glossário ---
# Os dois relatórios de gasto (fechamento do orçamento e meio de mês) usam o
# mesmo verbete de categoria; só o de carteira usa o de camada. Ficam separados
# para que cada montador componha o glossário do que de fato citou.

def glossario_categorias():
    itens = ["<h3>Categorias de gasto</h3><dl class='glossario'>"]
    for c in politica.CATEGORIAS:
        itens.append(f'<dt>{rotulo_chip(politica.COR_CAT[c], politica.ROTULO_CAT[c])}</dt>'
                     f'<dd>{politica.TEXTO_CATEGORIA[c]}</dd>')
    itens.append("</dl>")
    return "".join(itens)


def glossario_camadas():
    itens = ["<h3>Camadas de investimento</h3><dl class='glossario'>"]
    for cm in politica.CAMADAS_COM_ALVO + politica.CAMADAS_SEM_ALVO:
        itens.append(f'<dt>{rotulo_chip(politica.COR_CAMADA[cm], politica.ROTULO_CAMADA[cm])}</dt>'
                     f'<dd>{politica.TEXTO_CAMADA[cm]}</dd>')
    itens.append("</dl>")
    return "".join(itens)

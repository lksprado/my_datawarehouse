#!/usr/bin/env python3
"""Monta o HTML do relatório de meio de mês a partir do JSON extraído.

    montar_meio_mes.py --dados D.json --narrativa N.json --saida R.html

Um relatório só, do casal — despesa não tem lançamento por pessoa. Duas
partes, com recortes de tempo diferentes e deliberadamente não misturados:

    partes 1-4   mês CORRENTE, até o dia do corte: ritmo do gasto, projeção de
                 fechamento e a margem que ainda cabe gastar. É acionável
                 justamente porque o mês não acabou.
    parte 5      mês ANTERIOR: desempenho do patrimônio contra CDI e inflação
                 pessoal. Mora aqui, e não no relatório de fechamento, porque o
                 IPCA só sai por volta do dia 10 — no dia 1º o número não
                 existe.

Divisão de responsabilidades igual à do fechamento: este script renderiza tudo
que é calculável, e o arquivo de narrativa traz só o texto analítico.

Estrutura do arquivo de narrativa (todas as chaves opcionais):

    {
      "sumario": "<p>…</p>",
      "diagnostico_ritmo": "<p>…</p>",
      "diagnostico_categorias": "<p>…</p>",
      "diagnostico_desempenho": "<p>…</p>",   # só se pronto_indicadores
      "recomendacoes": [{"titulo": "…", "texto": "…"}],
      "premissas": ["…"]
    }
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts"))

from relatorios.blocos import (bloco, css_inline, glossario_categorias,  # noqa: E402
                               kpi, recomendacoes_html, rotulo_chip, secao,
                               tabela, tag)
from relatorios.calculo import mediana  # noqa: E402
from relatorios.formato import (brl, classe_delta, data_br, dia_mes_br, esc,  # noqa: E402
                                mes_curto, mes_extenso, num, pct, sinal)
from relatorios.graficos import (barras_desvio, legenda, linhas,  # noqa: E402
                                 linhas_dia)
from relatorios.politica import (CATEGORIAS, CATEGORIAS_COMPRIMIVEIS,  # noqa: E402
                                 COR_CAT, META_POUPANCA_PCT, ROTULO_CAT,
                                 SERIES)

# Marcas de referência: a envoltória e a mediana dos meses fechados não são
# entidades, são contexto. Vão em tinta neutra, e a mediana se distingue por
# traço, não por cor — cor categórica aqui faria o leitor contar três séries.
COR_REF = "#52514e"
# Acima deste desvio a projeção de uma categoria deixa de ser ruído.
BANDA_CATEGORIA_PCT = 15


# ------------------------------------------------------------------ cálculo ---

def curva_densa(pontos, dias, chave="acumulado"):
    """Acumulado dia a dia, preenchido para frente.

    A série do banco só tem linha nos dias em que houve lançamento. Num
    acumulado, dia sem lançamento repete o valor anterior — deixar buraco faria
    a linha cair até zero no gráfico."""
    por_dia = {int(p["dia_fatura"]): float(p[chave]) for p in pontos}
    saida, ultimo = [], 0.0
    for d in dias:
        ultimo = por_dia.get(d, ultimo)
        saida.append(ultimo)
    return saida


def envelope(historico, dias):
    """Mínimo, mediana e máximo por dia entre os meses fechados."""
    por_mes = {}
    for p in historico:
        por_mes.setdefault(p["mes"], []).append(p)
    curvas = [curva_densa(ps, dias) for ps in por_mes.values()]
    if not curvas:
        return None, None, None
    lo = [min(c[i] for c in curvas) for i in range(len(dias))]
    hi = [max(c[i] for c in curvas) for i in range(len(dias))]
    med = [mediana([c[i] for c in curvas]) for i in range(len(dias))]
    return lo, med, hi


def por_categoria(d):
    return {c["categoria"]: c for c in d["categorias"]}


# ------------------------------------------------------------------ montagem ---

def aviso_prontidao(meta):
    """Tarja de dado insuficiente. Dois portões independentes: um mês ainda
    cedo demais não impede a leitura de benchmark do mês anterior, e indexador
    não publicado não impede a leitura de ritmo."""
    pr = meta["prontidao"]
    avisos = []
    if not pr["pronto_ritmo"]:
        itens = "".join(f"<li>{esc(p)}</li>" for p in pr["pendencias_ritmo"])
        avisos.append(
            '<div class="destaque-bloco alerta"><h4>Ritmo do mês — base '
            'insuficiente</h4><p>O acumulado do mês corrente ainda não sustenta '
            'projeção. Os números das seções de ritmo, categorias e margem '
            'estão parciais e não devem embasar decisão.</p>'
            f'<ul>{itens}</ul></div>')
    if not pr["pronto_indicadores"]:
        itens = "".join(f"<li>{esc(p)}</li>" for p in pr["pendencias_indicadores"])
        ult = pr.get("ultimo_mes_indicador")
        alt = (f"<p>Último mês com indexador publicado: "
               f"<strong>{mes_extenso(ult)}</strong>.</p>") if ult else ""
        avisos.append(
            '<div class="destaque-bloco alerta"><h4>Desempenho — indexadores '
            'ainda não publicados</h4>'
            f'<ul>{itens}</ul>{alt}</div>')
    return "".join(avisos)


def cabecalho(meta):
    corte = f"01/{meta['mes_corrente'][5:7]} a {dia_mes_br(meta['hoje'])}"
    return f"""<meta charset="utf-8"><title>Acompanhamento de meio de mês</title>
<style>{css_inline()}</style>
<div class="capa">
  <div class="eyebrow">Acompanhamento de meio de mês · orçamento do casal</div>
  <h1>Acompanhamento de {mes_extenso(meta['mes_corrente'])}</h1>
  <div class="periodo">{esc(corte)} · faltam {meta['dias_restantes']} dias</div>
  <p style="max-width:120mm;color:var(--text-secondary)">
    Preparado para Lucas e Jéssica. Lê o gasto do mês <strong>em andamento</strong>
    contra o ritmo dos seis meses fechados, projeta o fechamento e diz quanto
    ainda cabe gastar — enquanto ainda dá para mudar o resultado.
    <br><br>
    A última seção fala de <strong>{mes_extenso(meta['mes_anterior'])}</strong>,
    não deste mês: é o desempenho do patrimônio contra os benchmarks, que só
    agora tem indexador publicado. O relatório de fechamento roda antes disso e
    por isso não o traz.
  </p>
  {aviso_prontidao(meta)}
  <div class="rodape-capa">
    Gerado em {data_br(meta['gerado_em'])} a partir da camada <code>marts</code>
    do data warehouse · corte no dia {meta['dia_corte']} do ciclo de fatura
  </div>
</div>"""


def secao_ritmo(d, n, num_secao):
    meta = d["meta"]
    dias = list(range(1, meta["dias_no_mes"] + 1))
    corte = meta["dia_corte"]
    dias_ate_corte = [x for x in dias if x <= corte]

    atual = curva_densa(d["ritmo"], dias_ate_corte)
    lo, med, hi = envelope(d["ritmo_historico"], dias)

    series = [(mes_curto(meta["mes_corrente"]), SERIES[0], atual, False)]
    if med:
        series.append(("Mediana 6m", COR_REF, med, True))

    marcos = [x for x in (5, 10, 15, 20, 25, dias[-1]) if x <= dias[-1]]
    linhas_tab = []
    for m in marcos:
        i = m - 1
        v_atual = atual[m - 1] if m <= corte else None
        v_med = med[i] if med else None
        var = ((v_atual - v_med) / v_med * 100
               if v_atual is not None and v_med else None)
        linhas_tab.append([
            f"dia {m}" + (" (corte)" if m == corte else ""),
            brl(v_atual) if v_atual is not None else "—",
            brl(v_med), brl(lo[i]) if lo else "—", brl(hi[i]) if hi else "—",
            f'<span class="{classe_delta(var, bom_se_sobe=False)}">{sinal(var)}</span>'])

    corpo = (
        f'<figure>{linhas_dia(dias, series, faixa=(lo, hi) if lo else None)}'
        + legenda([(f"Acumulado de {mes_curto(meta['mes_corrente'])}", SERIES[0]),
                   ("Mediana dos 6 meses fechados", COR_REF)])
        + '<figcaption>Gasto acumulado do mês, por dia do ciclo de fatura. A '
          'faixa cinza é o intervalo entre o menor e o maior acumulado dos seis '
          'meses fechados no mesmo dia. A linha do mês corrente termina no dia '
          'do corte — não há dado depois dele.</figcaption></figure>'
        + tabela(["Marco", f"{mes_curto(meta['mes_corrente'])}", "Mediana 6m",
                  "Mínimo 6m", "Máximo 6m", "Var. vs mediana"], linhas_tab)
        + '<p class="sub">O eixo é o dia do <strong>ciclo de fatura</strong> '
          '(<code>dia_fatura</code>), não o dia do calendário: é o que torna '
          'dois meses comparáveis dia a dia, porque a compra no cartão pertence '
          'a um mês e é paga em outro.</p>'
        + bloco("Leitura do ritmo", n.get("diagnostico_ritmo", "")))
    return secao(num_secao, "Ritmo do mês",
                 f"Acumulado até o dia {corte} do ciclo · contra os seis meses "
                 f"fechados", corpo)


def secao_categorias(d, n, num_secao):
    cats = por_categoria(d)
    tot = cats["total"]
    linhas_tab, desvios, soma_proj = [], [], 0.0
    for c in CATEGORIAS:
        r = cats.get(c)
        if not r:
            continue
        realizado, agendado = float(r["realizado"]), float(r["agendado"])
        mesmo, proj = float(r["mesmo_dia_mediana_6m"]), float(r["projecao"])
        cheio = float(r["mes_cheio_mediana_6m"])
        soma_proj += proj
        var_dia = ((realizado - mesmo) / mesmo * 100) if mesmo else None
        var_proj = ((proj - cheio) / cheio * 100) if cheio else None
        fora = var_proj is not None and abs(var_proj) > BANDA_CATEGORIA_PCT
        linhas_tab.append([
            rotulo_chip(COR_CAT[c], ROTULO_CAT[c]),
            brl(realizado), brl(mesmo),
            f'<span class="{classe_delta(var_dia, bom_se_sobe=False)}">{sinal(var_dia)}</span>',
            brl(agendado), brl(proj), brl(cheio),
            tag("acima", "tag-atencao", "▲") if fora and var_proj > 0 else
            tag("abaixo", "tag-ok", "✓") if fora else tag("no padrão", "", "•")])
        if var_proj is not None:
            desvios.append((ROTULO_CAT[c], var_proj,
                            f"{sinal(var_proj, 0)} · {brl(proj - cheio)}"))

    proj_total = float(tot["projecao"])
    cheio_total = float(tot["mes_cheio_mediana_6m"])
    # A mediana de uma soma não é a soma das medianas, e GREATEST é não linear:
    # a coluna de projeção não fecha com a linha de total, e isso é correto.
    # Silenciar seria pior — o leitor soma e desconfia do relatório inteiro.
    gap = soma_proj - proj_total
    nota_gap = (
        f'<p class="sub">A coluna de projeção <strong>não soma</strong> ao total '
        f'({brl(soma_proj)} contra {brl(proj_total)}, diferença de '
        f'{brl(abs(gap))}). Não é erro de conta: cada projeção usa a mediana da '
        f'categoria, e a mediana de uma soma não é a soma das medianas. Para '
        f'julgar o mês inteiro vale o total, apurado sobre a série do total; '
        f'para julgar uma categoria vale a linha dela.</p>')

    corpo = (
        tabela(["Categoria", f"Até o dia {d['meta']['dia_corte']}", "Mesmo dia 6m",
                "Var.", "Já agendado", "Projeção", "Mediana do mês 6m", "Situação"],
               linhas_tab,
               ["Total", brl(tot["realizado"]), brl(tot["mesmo_dia_mediana_6m"]),
                sinal(((float(tot["realizado"]) - float(tot["mesmo_dia_mediana_6m"]))
                       / float(tot["mesmo_dia_mediana_6m"]) * 100)
                      if float(tot["mesmo_dia_mediana_6m"]) else None),
                brl(tot["agendado"]), brl(proj_total), brl(cheio_total), ""],
               ["l", "n", "n", "n", "n", "n", "n", "l"])
        + nota_gap
        + "<h3>Onde a projeção destoa do mês típico</h3>"
        + f'<figure>{barras_desvio(sorted(desvios, key=lambda x: -x[1]))}'
          f'<figcaption>Desvio da projeção de fechamento contra a mediana do mês '
          f'cheio dos seis meses fechados.</figcaption></figure>'
        + f'<p class="sub">Regra da projeção: <strong>realizado até o corte + o '
          f'maior entre</strong> (a) a mediana do que caiu depois do dia do corte '
          f'nos seis meses fechados e (b) o que já está lançado com data futura '
          f'neste mês. É o maior dos dois, e não a soma, porque a mediana '
          f'histórica já embute as despesas fixas do dia 25 — somar contaria as '
          f'fixas duas vezes.</p>'
        + bloco("Leitura das categorias", n.get("diagnostico_categorias", "")))
    return secao(num_secao, "Categorias",
                 "Realizado, comprometido e projetado contra o mês típico",
                 corpo, quebra=True)


def secao_margem(d, n, num_secao):
    """A seção que só faz sentido no meio do mês: o que ainda dá para decidir.

    Só entram as categorias que a política admite comprimir. `saude` e
    `educacao` ficam de fora por decisão registrada no glossário, e as fixas
    (`apartamento`, `assinaturas`) não se comprimem dentro do mês."""
    cats = por_categoria(d)
    tot = cats["total"]
    dre = d["dre_mensal"]
    receita = float(dre[-1]["total_receita"]) if dre else 0.0
    proj_total = float(tot["projecao"])

    teto_poupanca = receita * (1 - META_POUPANCA_PCT / 100)
    folga_poupanca = teto_poupanca - proj_total
    poupanca_proj = (receita - proj_total) / receita * 100 if receita else 0

    linhas_tab = []
    for c in CATEGORIAS_COMPRIMIVEIS:
        r = cats.get(c)
        if not r:
            continue
        proj, cheio = float(r["projecao"]), float(r["mes_cheio_mediana_6m"])
        realizado = float(r["realizado"])
        folga = cheio - proj
        linhas_tab.append([
            rotulo_chip(COR_CAT[c], ROTULO_CAT[c]),
            brl(realizado), brl(proj), brl(cheio),
            f'<span class="{"delta-neg" if folga < 0 else ""}">{brl(folga)}</span>',
            tag("já estourou", "tag-atencao", "▲") if folga < 0
            else tag("ainda cabe", "tag-ok", "✓")])

    corpo = (
        '<div class="kpis tres">'
        + kpi("Poupança projetada", pct(poupanca_proj),
              f"meta {META_POUPANCA_PCT}%",
              "delta-pos" if poupanca_proj >= META_POUPANCA_PCT else "delta-neg")
        + kpi("Teto de despesa do mês", brl(teto_poupanca),
              f"para fechar em {META_POUPANCA_PCT}% de poupança")
        + kpi("Folga contra o teto", brl(folga_poupanca),
              "acima do teto" if folga_poupanca < 0 else "ainda dentro",
              "delta-pos" if folga_poupanca >= 0 else "delta-neg")
        + "</div>"
        + "<h3>Margem por categoria comprimível</h3>"
        + tabela(["Categoria", f"Até o dia {d['meta']['dia_corte']}", "Projeção",
                  "Mediana do mês 6m", "Ainda cabe", "Situação"], linhas_tab,
                 None, ["l", "n", "n", "n", "n", "l"])
        + '<p class="sub">Só entram aqui as categorias que a política admite '
          'comprimir. <strong>Saúde e educação não entram em sugestão de corte</strong>, '
          'por decisão registrada no glossário; apartamento e assinaturas são '
          'fixas e não se comprimem dentro do mês. «Ainda cabe» é a diferença '
          'entre a mediana do mês cheio e a projeção — quanto dá para gastar sem '
          'sair do padrão dos últimos seis meses.</p>')
    return secao(num_secao, "Margem disponível",
                 f"O que ainda dá para decidir nos {d['meta']['dias_restantes']} "
                 f"dias restantes", corpo)


def secao_desempenho(d, n, num_secao):
    """Seção do mês ANTERIOR. Migrada do relatório de fechamento, que roda antes
    de o IPCA sair. Nunca lê `comparativo_*`: aquelas colunas rotulam superação
    com RICO/POBRE, vocabulário interno que não vai para o PDF."""
    meta = d["meta"]
    r, ind = d["riqueza"], d["indicadores"]
    if not r:
        return secao(num_secao, f"Desempenho até {mes_extenso(meta['mes_anterior'])}",
                     "Patrimônio contra CDI e inflação pessoal",
                     "<p>Sem série de benchmark disponível para o período.</p>",
                     quebra=True)
    meses = [x["mes_base"] for x in r]
    u = r[-1]
    series = [("Patrimônio", SERIES[0],
               [float(x["total_patrimonio_liquido_acum"]) for x in r]),
              ("CDI", SERIES[1], [float(x["cdi_acum"]) for x in r]),
              ("Infl. pessoal", SERIES[2], [float(x["minha_inflacao_acum"]) for x in r])]
    ind_por_mes = {x["mes_base"]: x for x in ind}

    corpo = (
        f'<figure>{linhas(meses, series, formato="idx")}'
        f'<figcaption>Índice acumulado, base 1. Séries indexadas à mesma base — '
        f'eixo único.</figcaption></figure>'
        + tabela(["Mês", "Patrimônio", "CDI", "IPCA", "Infl. pessoal",
                  "IPCA no mês", "CDI no mês"],
                 [[mes_curto(x["mes_base"]), num(x["total_patrimonio_liquido_acum"]),
                   num(x["cdi_acum"]), num(x["ipca_acum"]),
                   num(x["minha_inflacao_acum"]),
                   pct(float(ind_por_mes[x["mes_base"]]["ipca"]) * 100, 2)
                   if x["mes_base"] in ind_por_mes
                   and ind_por_mes[x["mes_base"]]["ipca"] is not None else "—",
                   pct(float(ind_por_mes[x["mes_base"]]["cdi"]) * 100, 2)
                   if x["mes_base"] in ind_por_mes
                   and ind_por_mes[x["mes_base"]]["cdi"] is not None else "—"]
                  for x in r])
        + f'<p class="sub">Em {mes_extenso(meta["mes_anterior"])} o patrimônio '
          f'acumula índice {num(u["total_patrimonio_liquido_acum"])} contra '
          f'{num(u["cdi_acum"])} do CDI e {num(u["minha_inflacao_acum"])} da '
          f'inflação pessoal. <strong>O índice de patrimônio compõe aporte e '
          f'rentabilidade — não é retorno da carteira</strong>, e compará-lo ao '
          f'CDI superestima o desempenho. Serve para responder "o patrimônio '
          f'cresceu mais que a inflação?", não "a carteira bateu o CDI?".</p>'
        + bloco("", n.get("diagnostico_desempenho", "")))
    return secao(num_secao, f"Desempenho até {mes_extenso(meta['mes_anterior'])}",
                 "Patrimônio líquido do casal contra CDI e inflação pessoal · "
                 "índice acumulado base 1", corpo, quebra=True)


def rodape(d, premissas):
    meta = d["meta"]
    pr = meta["prontidao"]
    itens = "".join(f"<li>{esc(p)}</li>" for p in premissas)
    bloco_prem = (f"<p><strong>Premissas e critérios adotados neste "
                  f"relatório:</strong></p><ul>{itens}</ul>") if premissas else ""

    incompleto = ""
    for chave, rot, pend in (("pronto_ritmo", "Ritmo em base insuficiente", "pendencias_ritmo"),
                             ("pronto_indicadores", "Indexadores não publicados", "pendencias_indicadores")):
        if not pr[chave]:
            incompleto += (f"<p><strong>{rot}.</strong> "
                           f"{esc('; '.join(pr[pend]))}</p>")

    esp = meta.get("motivos_especiais")
    sazonal = (f"<p>{mes_extenso(meta['mes_corrente'])} é mês de data especial "
               f"({esc(esp.capitalize())}), o que costuma elevar rolê e "
               f"diversos.</p>") if esp else ""

    return f"""<section class="quebra"><h2><span class="num">—</span>Notas e procedência</h2>
<div class="rodape-doc">{incompleto}{bloco_prem}{sazonal}
  <p>Origem: camada <code>marts</code> do data warehouse pessoal (PostgreSQL),
  domínio finanças. Ritmo, categorias e margem apurados em
  {mes_extenso(meta['mes_corrente'])} até {data_br(meta['hoje'])}, no dia
  {meta['dia_corte']} do ciclo de fatura. Desempenho apurado em
  {mes_extenso(meta['mes_anterior'])}. A base de comparação são os
  {meta['meses_de_base']} meses fechados anteriores.</p>
  <p>Este relatório não traz posição de carteira, exposição ao FGC nem cobertura
  da reserva de emergência — esses são assunto dos relatórios de fechamento,
  gerados no início do mês.</p>
  <p>Definições de categoria de gasto conforme
  <code>models/marts/financas/_docs_financas.md</code>, fonte única do domínio.
  Meses posteriores ao corrente existem na base como lançamentos futuros
  pré-agendados e foram excluídos de todos os números.</p>
  <p>Este documento é gerado automaticamente a partir de dados próprios e não
  constitui recomendação de investimento de profissional certificado.</p>
  <p>Gerado em {data_br(meta['gerado_em'])}.</p>
</div></section>"""


def montar(d, n):
    meta = d["meta"]
    pr = meta["prontidao"]
    cats = por_categoria(d)
    tot = cats["total"]
    dre = d["dre_mensal"]
    receita = float(dre[-1]["total_receita"]) if dre else 0.0
    proj_total, realizado = float(tot["projecao"]), float(tot["realizado"])
    mesmo_dia = float(tot["mesmo_dia_mediana_6m"])
    cheio = float(tot["mes_cheio_mediana_6m"])
    var_dia = ((realizado - mesmo_dia) / mesmo_dia * 100) if mesmo_dia else None
    var_proj = ((proj_total - cheio) / cheio * 100) if cheio else None
    poupanca_proj = (receita - proj_total) / receita * 100 if receita else 0

    numero = 0

    def prox():
        nonlocal numero
        numero += 1
        return numero

    partes = [cabecalho(meta)]

    partes.append(secao(prox(), "Sumário executivo",
        f"{mes_extenso(meta['mes_corrente']).capitalize()} até o dia "
        f"{meta['dia_corte']} do ciclo",
        '<div class="kpis">'
        + kpi(f"Gasto até o dia {meta['dia_corte']}", brl(realizado),
              f"{sinal(var_dia)} vs. mesmo dia, mediana 6m",
              classe_delta(var_dia, bom_se_sobe=False))
        + kpi("Projeção de fechamento", brl(proj_total),
              f"{sinal(var_proj)} vs. mediana do mês 6m",
              classe_delta(var_proj, bom_se_sobe=False))
        + kpi("Poupança projetada", pct(poupanca_proj),
              f"meta {META_POUPANCA_PCT}%",
              "delta-pos" if poupanca_proj >= META_POUPANCA_PCT else "delta-neg")
        + kpi("Dias restantes", str(meta["dias_restantes"]),
              f"de {meta['dias_no_mes']} no mês")
        + "</div>"
        + (n.get("sumario") or "")
        + '<p class="sub">A receita do mês corrente já está lançada (é o '
          'salário) e sustenta a poupança projetada; a despesa é projeção, não '
          'realizado. Carteira, reserva de emergência e risco FGC não entram '
          'neste relatório — saem no fechamento.</p>'))

    partes.append(secao_ritmo(d, n, prox()))
    partes.append(secao_categorias(d, n, prox()))
    partes.append(secao_margem(d, n, prox()))
    if pr["pronto_indicadores"]:
        partes.append(secao_desempenho(d, n, prox()))

    partes.append(secao(prox(), "Recomendações",
        f"Ações para os {meta['dias_restantes']} dias que restam · o destino do "
        f"aporte e a alocação por camada saem no relatório de fechamento",
        recomendacoes_html(n.get("recomendacoes", [])), quebra=True))

    partes.append(secao(prox(), "Glossário",
        "O que cada categoria de gasto engloba",
        glossario_categorias(), quebra=True))

    partes.append(rodape(d, n.get("premissas", [])))
    return "\n".join(partes)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dados", required=True)
    ap.add_argument("--narrativa", required=True)
    ap.add_argument("--saida", required=True)
    a = ap.parse_args()

    d = json.loads(Path(a.dados).read_text(encoding="utf-8"))
    n = json.loads(Path(a.narrativa).read_text(encoding="utf-8"))
    html = montar(d, n)
    saida = Path(a.saida)
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(html, encoding="utf-8")
    print(f"HTML montado: {saida} ({len(html)//1024} KB)")


if __name__ == "__main__":
    main()

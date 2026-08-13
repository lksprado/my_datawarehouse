#!/usr/bin/env python3
"""Monta o HTML de um relatório financeiro a partir do JSON extraído.

    montar_relatorio.py --dados D.json --escopo orcamento|lucas|jessica|deusa \
                        --narrativa N.json --saida R.html

São quatro relatórios, separados por assunto e por titular:

    orcamento          casal — receita, despesa, resultado, poupança, gasto por
                       categoria, conta de luz e cobertura da reserva
    lucas | jessica    investimentos do titular — patrimônio, carteira, renda
                       passiva, riscos, aporte
    deusa              idem, sem patrimônio: não há dado dela em
                       marts.patrimonio

O desempenho contra CDI e inflação pessoal não sai aqui. Os indexadores do mês
só são publicados por volta do dia 10 e a planilha é preenchida depois — no
dia em que este relatório roda eles ainda não existem. A leitura de benchmark
é do relatório de meio de mês (`.claude/skills/relatorio-meio-mes/`).

Divisão de responsabilidades: este script renderiza tudo que é *calculável*
— KPIs, tabelas, gráficos SVG — direto dos dados, para que nenhum número do
PDF dependa de transcrição manual. O arquivo de narrativa traz apenas o texto
analítico (diagnóstico e recomendações), que é escrito pelo agente.

Estrutura do arquivo de narrativa (todas as chaves opcionais):

    {
      "sumario": "<p>…</p>",
      "diagnostico_orcamento": "<p>…</p>",   # só no escopo orcamento
      "diagnostico_carteira": "<p>…</p>",    # só nos escopos de investimento
      "diagnostico_riscos": "<p>…</p>",      # idem
      "recomendacoes": [{"titulo": "…", "texto": "…"}],
      "premissas": ["…"]
    }
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# A camada compartilhada com o relatório de meio de mês mora no repositório, em
# scripts/relatorios/ — parâmetros da política, formatação pt-BR, gráficos SVG e
# blocos de HTML. Aqui fica só o que é específico do fechamento: capa, rodapé,
# tarja de prontidão e a ordem das seções.
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts"))

from relatorios.blocos import (bloco, css_inline, glossario_camadas,  # noqa: E402
                              glossario_categorias, kpi, recomendacoes_html,
                              rotulo_chip, secao, tabela, tag_risco)
from relatorios.calculo import media, mediana, variacao  # noqa: E402
from relatorios.formato import (brl, classe_delta, data_br, esc,  # noqa: E402
                                mes_curto, mes_extenso, pct, sinal)
from relatorios.graficos import (barras_agrupadas, barras_alvo,  # noqa: E402
                                 barras_empilhadas, legenda)
from relatorios.politica import (ALVOS_CAMADA, APORTE_ALVO,  # noqa: E402
                                 CAMADAS_COM_ALVO, CAMADAS_SEM_ALVO,
                                 CATEGORIAS, COM_PATRIMONIO, COR_CAMADA,
                                 COR_CAT, ESCOPOS_INVESTIMENTO,
                                 META_INTERNACIONAL_PCT, META_RESERVA_MESES,
                                 META_RESERVA_PISO, NOME, ROTULO_CAMADA,
                                 ROTULO_CAT, SERIES, TEXTO_CAMADA)

# ----------------------------------------------------------------- carteira ---

def nota_bases(base_alvo, total):
    """A tabela de camadas usa duas bases; sem esta nota ela engana."""
    if base_alvo >= total:
        return ""
    return (f"<p class='sub'>As camadas com alvo somam 100% da base de "
            f"rebalanceamento ({brl(base_alvo)}), que exclui reserva "
            f"estratégica e não classificados. As duas últimas linhas estão em "
            f"% da carteira total ({brl(total)}).</p>")


def secao_carteira(d, pessoa):
    camadas = [c for c in d["carteira_camada"] if c["pessoa"] == pessoa]
    total = sum(c["valor"] for c in camadas)
    if not total:
        return ""
    alvos = ALVOS_CAMADA[pessoa]
    valores = {c["camada"]: c["valor"] for c in camadas}

    # O alvo da política é sobre a carteira SEM as camadas que não têm alvo, e
    # `pct_da_carteira` vem do SQL calculado sobre o total — por isso o
    # percentual comparável ao alvo é recalculado aqui sobre a base-alvo.
    base_alvo = total - sum(valores.get(cm, 0) for cm in CAMADAS_SEM_ALVO)
    dados_alvo = [(cm, COR_CAMADA[cm],
                   float(valores.get(cm, 0)) / base_alvo * 100 if base_alvo else 0.0,
                   alvos.get(cm, 0))
                  for cm in CAMADAS_COM_ALVO]
    # Estas seguem em % da carteira total — é o valor que elas de fato ocupam.
    atual_total = {c["camada"]: float(c["pct_da_carteira"]) for c in camadas}

    lin = []
    for cm, cor, a, alvo in dados_alvo:
        desvio = a - alvo
        dentro = abs(desvio) <= 5
        lin.append([
            rotulo_chip(cor, ROTULO_CAMADA[cm]),
            brl(valores.get(cm, 0)), pct(a), f"{alvo}%",
            # desvio do alvo é ruim nos dois sentidos — nunca verde por ser positivo
            f'<span class="{"" if dentro else "delta-neg"}">'
            f'{sinal(desvio, 0, " p.p.")}</span>',
            '<span class="tag tag-ok">✓ na banda</span>' if dentro else
            '<span class="tag tag-atencao">▲ fora da banda</span>',
        ])
    # Camadas sem alvo: entram na tabela para que o valor não desapareça do
    # relatório, mas com alvo "—" e percentual sobre a carteira toda.
    for cm in CAMADAS_SEM_ALVO:
        if not valores.get(cm):
            continue
        pendencia = cm == "NAO CLASSIFICADO"
        lin.append([
            rotulo_chip(COR_CAMADA[cm], ROTULO_CAMADA[cm]),
            brl(valores[cm]), pct(atual_total.get(cm, 0.0)), "—", "—",
            '<span class="tag tag-atencao">▲ classificar</span>' if pendencia
            else '<span class="tag">sem alvo</span>'])

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
        kpi("Exposição em dólar", pct(moedas.get("USD", 0)),
            f"alvo {META_INTERNACIONAL_PCT[pessoa]}%"),
        "</div>",
        "<h3>Alocação por camada contra a política</h3>",
        f"<figure>{barras_alvo(dados_alvo)}</figure>",
        tabela(["Camada", "Valor", "Atual", "Alvo", "Desvio", "Situação"], lin),
        nota_bases(base_alvo, total),
        "<h3>Concentração</h3>",
        tabela(["Instituição", "Valor", "% da carteira"],
               [[i["instituicao"].title(), brl(i["valor"]), pct(i["pct_da_carteira"])]
                for i in inst],
               ["Total", brl(total), "100,0%"]),
        tabela(["Categoria", "Tipo", "Valor", "% da carteira"],
               [[t["classe_ativo"].title(), t["tipo_ativo"].title(),
                 brl(t["valor"]), pct(t["pct_da_carteira"])] for t in tipos],
               None, ["l", "l", "n", "n"]),
        "<h3>Posições</h3>",
        tabela(["Investimento", "Camada", "Instituição", "Indexador", "Vencimento", "Valor"],
               [[esc(p["ativo"]), p["camada"].capitalize(),
                 p["instituicao"].title(), p["indexador"] or "—",
                 data_br(p["data_vencimento"]) if p["data_vencimento"] else "—",
                 brl(p["vlr_atualizado_brl"])] for p in posicoes],
               ["Total", "", "", "", "", brl(total)],
               ["l", "l", "l", "l", "l", "n"]),
    ]
    return "".join(html)


# ------------------------------------------------------------------ montagem ---

def aviso_prontidao(meta, escopo):
    """Tarja de dado incompleto. Só aparece quando o relatório foi gerado
    contra a recusa do portão — o número está incompleto e o leitor precisa
    saber disso antes de qualquer conclusão.

    Os portões são dois e independentes: o relatório de carteira não carrega
    tarja de mês de gasto não fechado, e vice-versa."""
    pr = meta.get("prontidao") or {}
    familia = "orcamento" if escopo == "orcamento" else "investimentos"
    if pr.get(f"pronto_{familia}", pr.get("pronto", True)):
        return ""
    itens = "".join(f"<li>{esc(p)}</li>"
                    for p in pr.get(f"pendencias_{familia}") or [])
    if familia == "orcamento":
        titulo = "Dados incompletos — mês não fechado"
        corpo = ("<p>Este relatório foi gerado sobre um mês de referência que "
                 "ainda não terminou de carregar. Totais, médias e taxa de "
                 "poupança estão subestimados e nenhuma comparação com meses "
                 "anteriores é válida.</p>")
        fechado = pr.get("ultimo_mes_fechado")
        alt = (f"<p>Último mês integralmente carregado: "
               f"<strong>{mes_extenso(fechado)}</strong>.</p>") if fechado else ""
    else:
        titulo = "Carteira defasada"
        corpo = (f"<p>A posição de carteira mais recente é de "
                 f"<strong>{mes_extenso(meta['mes_carteira'])}</strong>, "
                 f"{meta['defasagem_carteira_meses']} meses atrás do mês de "
                 f"referência. Os valores são reais, mas não são os de "
                 f"{mes_extenso(meta['mes_ref'])}.</p>")
        alt = ""
    return (f'<div class="destaque-bloco alerta"><h4>{titulo}</h4>'
            f'{corpo}<ul>{itens}</ul>{alt}</div>')


def cabecalho(titulo, subtitulo, meta, destinatarios, escopo):
    css = css_inline()
    defasagem = meta["defasagem_carteira_meses"]
    if escopo == "orcamento":
        conteudo = ("Reúne a apuração do orçamento do período — receita, "
                    "despesa, resultado e composição do gasto por categoria — "
                    "e a cobertura da reserva de emergência.")
        # A reserva é o único número do relatório que vem da carteira, e a
        # carteira fecha em cadência própria: sem esta nota o leitor supõe que
        # tudo está apurado no mesmo mês.
        aviso = (f"<p style='margin-top:4mm'><strong>Atenção às datas.</strong> "
                 f"Orçamento apurado em {mes_extenso(meta['mes_ref'])}; a "
                 f"reserva de emergência é lida da carteira, posicionada em "
                 f"{mes_extenso(meta['mes_carteira'])}.</p>") if defasagem else ""
    else:
        conteudo = ("Reúne o patrimônio do titular, a posição consolidada da "
                    "carteira, a renda passiva do período, a leitura de riscos "
                    "e o destino recomendado para o aporte."
                    if escopo in COM_PATRIMONIO else
                    "Reúne a posição consolidada da carteira, a renda passiva "
                    "do período, a leitura de riscos e o destino recomendado "
                    "para o aporte.")
        aviso = (f"<p style='margin-top:4mm'><strong>Atenção às datas.</strong> "
                 f"Todos os números estão posicionados em "
                 f"{mes_extenso(meta['mes_carteira'])}, que é o último "
                 f"fechamento de carteira disponível — {defasagem} mês atrás do "
                 f"mês de referência do relatório.</p>") if defasagem else ""
    return f"""<meta charset="utf-8"><title>{esc(titulo)}</title><style>{css}</style>
<div class="capa">
  <div class="eyebrow">Relatório de planejamento financeiro e investimentos</div>
  <h1>{esc(titulo)}</h1>
  <div class="periodo">{esc(subtitulo)}</div>
  <p style="max-width:120mm;color:var(--text-secondary)">
    Preparado para {esc(destinatarios)}. {conteudo}{aviso}
  </p>
  {aviso_prontidao(meta, escopo)}
  <div class="rodape-capa">
    Gerado em {data_br(meta['gerado_em'])} a partir da camada <code>marts</code>
    do data warehouse · mês de referência {meta['mes_ref'][:7]}
  </div>
</div>"""


def rodape(meta, premissas, escopo):
    pr = ""
    if premissas:
        itens = "".join(f"<li>{esc(p)}</li>" for p in premissas)
        pr = (f"<p><strong>Premissas e critérios adotados neste "
              f"relatório:</strong></p><ul>{itens}</ul>")

    prd = meta.get("prontidao") or {}
    familia = "orcamento" if escopo == "orcamento" else "investimentos"
    incompleto = ""
    if not prd.get(f"pronto_{familia}", prd.get("pronto", True)):
        pend = "; ".join(prd.get(f"pendencias_{familia}") or [])
        rotulo = ("Mês não fechado" if familia == "orcamento"
                  else "Carteira defasada")
        incompleto = (f"<p><strong>{rotulo}.</strong> O portão de prontidão "
                      f"reprovou {mes_extenso(meta['mes_ref'])} e o relatório foi "
                      f"gerado mesmo assim: {esc(pend)}</p>")

    # Data especial explica pico de rolê e diversos — só interessa ao orçamento.
    esp = meta.get("motivos_especiais")
    sazonal = (f"<p>{mes_extenso(meta['mes_ref'])} é mês de data especial "
               f"({esc(esp.capitalize())}), o que costuma elevar rolê e "
               f"diversos.</p>") if esp and escopo == "orcamento" else ""

    if escopo == "orcamento":
        procedencia = (
            f"Orçamento e consumo em {mes_extenso(meta['mes_ref'])}; a reserva "
            f"de emergência sai da carteira, posicionada em "
            f"{mes_extenso(meta['mes_carteira'])}.")
    elif escopo in COM_PATRIMONIO:
        procedencia = (
            f"Patrimônio, carteira e renda passiva em "
            f"{mes_extenso(meta['mes_carteira'])}. Este relatório não apura "
            f"orçamento: receita e despesa são lançadas em conjunto para o "
            f"casal e saem no relatório de orçamento. O desempenho contra CDI "
            f"e inflação pessoal sai no relatório de meio de mês, quando os "
            f"indexadores do mês já foram publicados.")
    else:
        procedencia = (
            f"Carteira e renda passiva em {mes_extenso(meta['mes_carteira'])}. "
            f"Não há lançamento de receita ou despesa registrado no warehouse "
            f"para esta carteira, portanto não há apuração de orçamento.")

    return f"""<section class="quebra"><h2><span class="num">—</span>Notas e procedência</h2>
<div class="rodape-doc">{incompleto}{pr}{sazonal}
  <p>Origem: camada <code>marts</code> do data warehouse pessoal (PostgreSQL),
  domínio finanças. {procedencia}</p>
  <p>Definições de categoria de gasto e de camada de investimento conforme
  <code>models/marts/financas/_docs_financas.md</code>, fonte única do domínio.
  Meses posteriores ao de referência existem na base como lançamentos futuros
  pré-agendados e foram excluídos de todos os números.</p>
  <p>Este documento é gerado automaticamente a partir de dados próprios e não
  constitui recomendação de investimento de profissional certificado.</p>
  <p>Gerado em {data_br(meta['gerado_em'])}.</p>
</div></section>"""


def glossario(escopo):
    """Só entra no glossário o vocabulário que o relatório de fato usa: o de
    orçamento fala de categoria de gasto e cita uma única camada, a reserva."""
    if escopo == "orcamento":
        return (glossario_categorias()
                + "<h3>Camada citada</h3><dl class='glossario'>"
                f'<dt>{rotulo_chip(COR_CAMADA["RESERVA"], ROTULO_CAMADA["RESERVA"])}</dt>'
                f'<dd>{TEXTO_CAMADA["RESERVA"]}</dd></dl>'
                "<p class='sub'>As demais camadas de investimento estão nos "
                "relatórios individuais de carteira.</p>")
    return (glossario_camadas()
            + "<p class='sub'>A camada descreve o papel do ativo na carteira, "
              "não o que ele é. Um CDB pode ser reserva ou crescimento conforme "
              "sua liquidez e exposição a marcação a mercado.</p>")


def montar_orcamento(d, n):
    """Relatório do casal: receita, despesa e o que sobra. Carteira e
    patrimônio saem nos relatórios individuais — aqui a carteira entra só pela
    reserva de emergência, que é dimensionada pela despesa do casal."""
    meta = d["meta"]
    dre = d["dre_mensal"]
    ult = dre[-1]
    meses = [r["mes_debito"] for r in dre]

    # Média para comparar o gasto do mês; mediana para dimensionar a reserva —
    # a política pede mediana para que um mês atípico não infle a meta.
    desp6 = media([r["total_despesas"] for r in dre[-7:-1]])
    desp6_med = mediana([r["total_despesas"] for r in dre[-7:-1]])
    rec6 = media([r["total_receita"] for r in dre[-7:-1]])
    poupanca = ult["resultado"] / ult["total_receita"] * 100 if ult["total_receita"] else 0
    poupanca12 = (sum(r["resultado"] for r in dre) /
                  sum(r["total_receita"] for r in dre) * 100)

    reserva_pessoa = {p: sum(c["valor"] for c in d["carteira_camada"]
                             if c["pessoa"] == p and c["camada"] == "RESERVA")
                      for p in ("lucas", "jessica")}
    reserva_casal = sum(reserva_pessoa.values())
    # Reserva-alvo = max(N x mediana da despesa de 6 meses, piso em R$).
    cobertura = reserva_casal / desp6_med if desp6_med else 0
    por_meses = META_RESERVA_MESES["casal"] * desp6_med
    meta_reserva = max(por_meses, META_RESERVA_PISO)
    meta_meses = meta_reserva / desp6_med if desp6_med else 0

    partes = [cabecalho(
        "Relatório de orçamento do casal",
        f"{mes_extenso(meta['mes_ref']).capitalize()}",
        meta, "Lucas e Jéssica", escopo="orcamento")]

    # 1 — sumário
    partes.append(secao(1, "Sumário executivo",
        f"Apuração de {mes_extenso(meta['mes_ref'])}",
        '<div class="kpis">'
        + kpi("Receita do mês", brl(ult["total_receita"]),
              f'{sinal(variacao(ult["total_receita"], rec6))} vs. média 6m',
              classe_delta(variacao(ult["total_receita"], rec6)))
        + kpi("Resultado do mês", brl(ult["resultado"]),
              f"{pct(poupanca)} da receita")
        + kpi("Despesa total", brl(ult["total_despesas"]),
              f'{sinal(variacao(ult["total_despesas"], desp6))} vs. média 6m',
              classe_delta(variacao(ult["total_despesas"], desp6), bom_se_sobe=False))
        + kpi("Reserva de emergência", f"{cobertura:.1f}".replace(".", ",") + " meses",
              f'meta {meta_meses:.0f} meses ({brl(meta_reserva)})',
              "delta-pos" if reserva_casal >= meta_reserva else "delta-neg")
        + "</div>"
        + (n.get("sumario") or "")))

    # 2 — resultado
    partes.append(secao(2, "Resultado do mês",
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

    # 3 — gastos por categoria
    series_cat = [(ROTULO_CAT[c], COR_CAT[c], [r[f"total_{c}"] for r in dre])
                  for c in CATEGORIAS]
    lin_cat = []
    for c in CATEGORIAS:
        atual = ult[f"total_{c}"]
        m6 = media([r[f"total_{c}"] for r in dre[-7:-1]])
        part = atual / ult["total_despesas"] * 100 if ult["total_despesas"] else 0
        v = variacao(atual, m6)
        lin_cat.append([
            rotulo_chip(COR_CAT[c], ROTULO_CAT[c]),
            brl(atual), pct(part), brl(m6),
            f'<span class="{classe_delta(v, bom_se_sobe=False)}">{sinal(v)}</span>'])
    partes.append(secao(3, "Gastos por categoria",
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

    # 4 — reserva de emergência
    # A reserva é a única ponte entre orçamento e carteira: o tamanho dela é
    # ditado pela despesa, mas o saldo mora na camada RESERVA dos dois titulares.
    regra = (f"{META_RESERVA_MESES['casal']} × a mediana da despesa de 6 meses "
             f"({brl(desp6_med)})" if por_meses >= META_RESERVA_PISO
             else f"o piso de {brl(META_RESERVA_PISO)}")
    falta = meta_reserva - reserva_casal
    situacao = (f"Faltam {brl(falta)} para a reserva-alvo."
                if falta > 0 else
                f"A reserva já supera o alvo em {brl(-falta)}.")
    partes.append(secao(4, "Reserva de emergência",
        f"Camada reserva dos dois titulares · posição em "
        f"{mes_extenso(meta['mes_carteira'])}",
        '<div class="kpis tres">'
        + kpi("Reserva atual", brl(reserva_casal),
              f"{cobertura:.1f}".replace(".", ",") + " meses de despesa")
        + kpi("Reserva-alvo", brl(meta_reserva), f"{meta_meses:.0f} meses")
        + kpi("Situação", "coberta" if falta <= 0 else "a completar",
              situacao, "delta-pos" if falta <= 0 else "delta-neg")
        + "</div>"
        + tabela(["Titular", "Reserva"],
                 [[NOME[p], brl(v)] for p, v in reserva_pessoa.items()],
                 ["Total", brl(reserva_casal)])
        + f'<p class="sub">Reserva-alvo = maior entre '
          f'{META_RESERVA_MESES["casal"]} × a mediana da despesa dos 6 meses '
          f'anteriores e o piso de {brl(META_RESERVA_PISO)}. Neste mês está '
          f'valendo {regra}. A mediana é usada no lugar da média para que um '
          f'mês atípico não infle a meta. A composição de cada reserva está no '
          f'relatório de investimentos do titular.</p>'))

    # 5 — recomendações
    partes.append(secao(5, "Recomendações",
        "Ações sobre gasto, poupança e reserva · o destino do aporte por camada "
        "está nos relatórios de investimento",
        recomendacoes_html(n.get("recomendacoes", [])), quebra=True))

    # 6 — glossário
    partes.append(secao(6, "Glossário",
        "O que cada categoria de gasto engloba",
        glossario("orcamento"), quebra=True))

    partes.append(rodape(meta, n.get("premissas", []), escopo="orcamento"))
    return "\n".join(partes)


def bloco_riscos(d, pessoas):
    fgc = [f for f in d["fgc"] if f["pessoa"] in pessoas]
    venc = [v for v in d["vencimentos_12m"] if v["pessoa"] in pessoas]
    nc = [x for x in d["nao_classificados"] if x["pessoa"] in pessoas]
    h = ["<h3>Exposição ao FGC por conglomerado</h3>",
         '<p class="sub">Garantia de R$ 250.000 por CPF e por conglomerado. '
         'A política exige folga mínima de R$ 50.000.</p>',
         tabela(["Titular", "Conglomerado", "Valor coberto restante", "Situação"],
                [[NOME[f["pessoa"]], f["conglomerado"].title(),
                  brl(f["vlr_liberado"]), tag_risco(f["risco_fgc"])] for f in fgc],
                None, ["l", "l", "n", "l"])]
    h.append("<h3>Vencimentos nos próximos 12 meses</h3>")
    if venc:
        h.append(tabela(["Titular", "Investimento", "Camada", "Vencimento", "Em", "Valor"],
                        [[NOME[v["pessoa"]], esc(v["ativo"]),
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
                        [[NOME[x["pessoa"]], esc(x["ativo"]),
                          x["instituicao"].title(), brl(x["vlr_atualizado_brl"])]
                         for x in nc]))
        h.append('<p class="sub">Classificar na aba <code>classificacao</code> '
                 'da planilha — esses valores ficam fora dos percentuais por camada.</p>')
    else:
        h.append("<p>Nenhum. Todos os ativos estão classificados por camada.</p>")
    return "".join(h)


def montar_investimentos(d, n, pessoa):
    """Relatório de investimentos de um titular. A seção de patrimônio só
    existe para quem tem série em marts.patrimonio, então a numeração é
    sequencial e não literal: o relatório de Deusa não pode sair com buraco
    de seção."""
    meta = d["meta"]
    tem_patrimonio = pessoa in COM_PATRIMONIO
    camadas = [c for c in d["carteira_camada"] if c["pessoa"] == pessoa]
    total = sum(c["valor"] for c in camadas)
    div = [r for r in d["dividendos"] if r["pessoa"] == pessoa]
    mdiv = sorted({r["mes_base"] for r in div})
    sv = [sum(r["vlr_liquido_brl"] for r in div if r["mes_base"] == m) for m in mdiv]
    tot12 = sum(sv)
    reserva = next((c["valor"] for c in camadas if c["camada"] == "RESERVA"), 0)

    pat = d["patrimonio"] if tem_patrimonio else []
    col_pat = f"patrimonio_liquido_{pessoa}"
    upat = pat[-1][col_pat] if pat else None
    var_pat = (variacao(pat[-1][col_pat], pat[-2][col_pat])
               if len(pat) > 1 else None)

    numero = 0

    def prox():
        nonlocal numero
        numero += 1
        return numero

    partes = [cabecalho("Relatório de investimentos",
                        mes_extenso(meta["mes_carteira"]).capitalize(),
                        meta, NOME[pessoa], escopo=pessoa)]

    # 1 — sumário. Quatro KPIs sempre: quem tem patrimônio abre com ele, quem
    # não tem abre com a reserva, para não deixar buraco no grid.
    primeiro_kpi = (kpi("Patrimônio líquido", brl(upat),
                        f"{sinal(var_pat)} vs. mês anterior", classe_delta(var_pat))
                    if tem_patrimonio else
                    kpi("Reserva", brl(reserva),
                        f"{reserva/total*100:.0f}% da carteira" if total else ""))
    nota_sem_orcamento = (
        '<p class="sub">Este relatório cobre exclusivamente investimentos. '
        'Não há lançamentos de receita e despesa registrados para '
        f'{NOME[pessoa]} no data warehouse, portanto não há apuração de '
        'orçamento.</p>' if not tem_patrimonio else
        '<p class="sub">Este relatório cobre exclusivamente investimentos. '
        'Receita, despesa e a cobertura da reserva de emergência são lançadas '
        'em conjunto para o casal e saem no relatório de orçamento.</p>')
    partes.append(secao(prox(), "Sumário executivo",
        f"Posição da carteira em {mes_extenso(meta['mes_carteira'])}",
        '<div class="kpis">'
        + primeiro_kpi
        + kpi("Carteira total", brl(total))
        + kpi("Renda passiva 12m", brl(tot12), f"média {brl(tot12/max(len(mdiv),1))}/mês")
        + kpi("Yield da carteira", pct(tot12 / total * 100 if total else 0), "12 meses")
        + "</div>"
        + (n.get("sumario") or "")
        + nota_sem_orcamento))

    # 2 — patrimônio (só quem tem série)
    if tem_patrimonio:
        partes.append(secao(prox(), "Patrimônio",
            f"Patrimônio líquido de {NOME[pessoa]} · últimos 12 meses",
            f'<figure>{barras_empilhadas([r["mes_base"] for r in pat], [(NOME[pessoa], SERIES[0], [r[col_pat] for r in pat])])}'
            f'<figcaption>Patrimônio líquido do titular, em reais.</figcaption></figure>'
            + tabela(["Mês", "Patrimônio líquido", "Var. mês", "% do casal"],
                     [[mes_curto(r["mes_base"]), brl(r[col_pat]),
                       f'<span class="{classe_delta(variacao(r[col_pat], pat[i-1][col_pat]) if i else None)}">'
                       f'{sinal(variacao(r[col_pat], pat[i-1][col_pat]) if i else None)}</span>',
                       pct(r[col_pat] / r["total_patrimonio_liquido"] * 100
                           if r["total_patrimonio_liquido"] else 0)]
                      for i, r in enumerate(pat)])))

    # 3 — carteira
    partes.append(secao(prox(), "Carteira",
        f"Posição em {mes_extenso(meta['mes_carteira'])}",
        secao_carteira(d, pessoa) + bloco("Leitura da carteira",
                                          n.get("diagnostico_carteira", "")),
        quebra=True))

    # 4 — renda passiva. Carteira sem provento rende gráfico e tabela de zeros —
    # uma frase diz a mesma coisa e não ocupa página.
    if tot12:
        corpo_renda = (
            f'<figure>{barras_empilhadas(mdiv, [("Proventos", SERIES[2], sv)], altura=180)}'
            f'<figcaption>Proventos líquidos recebidos por mês, em reais.'
            f'</figcaption></figure>'
            + tabela(["Mês", "Recebido"],
                     [[mes_curto(m), brl(sv[i])] for i, m in enumerate(mdiv)],
                     ["Total 12 meses", brl(tot12)]))
    else:
        corpo_renda = ("<p>Nenhum provento recebido nos últimos 12 meses. A "
                       "carteira não tem posição pagadora de renda — o que é "
                       "coerente com uma alocação-alvo sem camada de renda, mas "
                       "vira lacuna se o alvo mudar.</p>")
    partes.append(secao(prox(), "Renda passiva",
        "Dividendos e proventos líquidos · últimos 12 meses",
        corpo_renda, quebra=True))

    # 5 — riscos
    partes.append(secao(prox(), "Riscos", "FGC, concentração e vencimentos",
        bloco_riscos(d, [pessoa]) + bloco("", n.get("diagnostico_riscos", ""))))

    # 6 — recomendações
    partes.append(secao(prox(), "Recomendações",
        f"Destino sugerido para o aporte · aporte-alvo "
        f"{brl(APORTE_ALVO[pessoa])}/mês",
        recomendacoes_html(n.get("recomendacoes", [])), quebra=True))

    # 7 — glossário
    partes.append(secao(prox(), "Glossário",
        "O que significa cada camada de investimento",
        glossario(pessoa)))

    partes.append(rodape(meta, n.get("premissas", []), escopo=pessoa))
    return "\n".join(partes)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dados", required=True)
    ap.add_argument("--escopo", required=True,
                    choices=["orcamento"] + ESCOPOS_INVESTIMENTO)
    ap.add_argument("--narrativa", required=True)
    ap.add_argument("--saida", required=True)
    a = ap.parse_args()

    d = json.loads(Path(a.dados).read_text(encoding="utf-8"))
    n = json.loads(Path(a.narrativa).read_text(encoding="utf-8"))
    html = (montar_orcamento(d, n) if a.escopo == "orcamento"
            else montar_investimentos(d, n, a.escopo))
    saida = Path(a.saida)
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(html, encoding="utf-8")
    print(f"HTML montado: {saida} ({len(html)//1024} KB)")


if __name__ == "__main__":
    main()

---
name: relatorio-financas
description: Varre a camada marts do domínio finanças e gera dois relatórios mensais em PDF — um conjunto para Lucas e Jéssica (patrimônio, DRE, gastos por categoria e carteira) e um individual para Deusa (carteira e renda passiva). Use quando o usuário pedir o relatório financeiro do mês, o fechamento mensal, ou o PDF de finanças.
---

# Relatório financeiro mensal

Produz dois PDFs no padrão de um planejador financeiro pessoal somado a um
assessor de investimentos: diagnóstico do mês, leitura da carteira e destino
recomendado para o aporte.

| Relatório | Pessoas | Conteúdo |
|---|---|---|
| `relatorio_lucas_jessica_AAAA-MM.pdf` | Lucas + Jéssica (casal) | Patrimônio, DRE, gastos por categoria, carteira, aporte |
| `relatorio_deusa_AAAA-MM.pdf` | Deusa | Carteira, renda passiva, FGC, vencimentos, aporte |

Deusa não tem lançamento de despesa no warehouse — só carteira e dividendos.
O relatório dela é de investimentos, sem seção de orçamento. Não invente gastos.

**Acionamento: sob demanda, só.** Não há agendador — nem cron, nem DAG. O
usuário pede, você roda. Por isso o mês de referência é decidido aqui dentro
(Passo 1) e o mês incompleto é barrado aqui dentro (Passo 2). Para refazer
meses antigos em lote, sem sessão interativa, existe
`scripts/gerar_relatorios_financas.sh`.

## Passo 0 — Ler as regras do domínio

**Antes de qualquer análise**, leia `models/marts/financas/_docs_financas.md`.

É a fonte única do que cada categoria de gasto engloba, do que cada camada de
investimento significa e da política de investimento (aporte alvo, alocação-alvo
por camada, metas de reserva, limites e benchmark). Todo diagnóstico e toda
recomendação saem de lá. Se um parâmetro estiver marcado `[CONFIRMAR]`, use-o
mesmo assim e sinalize no relatório que é premissa a validar — não o substitua
por um número inventado.

Os parâmetros numéricos da política estão duplicados em `montar_relatorio.py`
(`alvos_camada()`, `APORTE_ALVO`, `META_RESERVA_*`, `TEXTO_CATEGORIA`,
`TEXTO_CAMADA`), porque o montador não lê Markdown. Se os dois discordarem,
**vale o `_docs_financas.md`** — e corrija o Python na mesma passada.

## Passo 1 — Resolver o mês de referência

O relatório fala sempre de **um mês fechado**, e é acionado sob demanda — não há
DAG passando a data. Descobrir de que mês se trata é o primeiro trabalho.

```bash
date +%Y-%m-%d
```

**Rode o comando. Não use a data que você acha que é** — a data do contexto pode
estar velha, e o mês errado contamina cada número em silêncio.

Regra, em ordem:

1. **Mês pedido pelo usuário vence.** Mês sem ano ("relatório de junho") é a
   ocorrência mais recente que não está no futuro.
2. Sem pedido explícito, é o mês anterior ao corrente:
   `date -d "$(date +%Y-%m-01) -1 month" +%Y-%m`.
3. **Nunca o mês corrente por default.** Ele está aberto: só tem gasto até hoje,
   e toda média, variação e taxa de poupança sai subestimada.

Declare o mês resolvido antes de extrair, para o usuário poder corrigir.

## Passo 2 — Extrair os dados

```bash
.claude/skills/relatorio-financas/scripts/extrair_dados.sh <AAAA-MM> <saida.json>
```

Grave o JSON no diretório de scratchpad da sessão, não no repositório.

**Todo número dos relatórios sai desse JSON.** Não consulte o banco por fora nem
recalcule agregados de cabeça — se faltar um corte, acrescente o bloco em
`queries/extrair.sql` e rode de novo.

### Portão de prontidão — antes de qualquer análise

Leia `meta.prontidao`. Se `pronto` for **falso**, **pare**: relate as
`pendencias` em português claro, diga qual foi o último mês fechado
(`ultimo_mes_fechado`) e ofereça gerar aquele. Não decida sozinho gerar mesmo
assim.

O portão distingue três situações que, nos números, se parecem:

| situação | como aparece |
|---|---|
| mês fechado | 26–28 dias com gasto, lançamento até o último dia |
| mês em andamento | lançamentos param no dia de hoje |
| mês pré-lançado | só as fixas (dia 25), `role`/`diversos`/`transporte` zerados |

Se o usuário mandar gerar mesmo assim, siga — o montador imprime a tarja de
dados incompletos na capa e no rodapé automaticamente, e o diagnóstico precisa
dizer que os valores estão parciais.

### Armadilhas dos dados — leia antes de interpretar

- **`marts.resultado` e `marts.consumo` contêm meses futuros pré-lançados** com
  as despesas fixas recorrentes já agendadas. Não são realizados. A extração já
  corta em `mes_ref`; nunca reintroduza meses posteriores no diagnóstico nem em
  médias.
- **A carteira fecha em cadência própria** e costuma estar 1 mês atrás do DRE.
  `meta.defasagem_carteira_meses` diz o quanto. Quando for maior que zero,
  declare no relatório a data de cada número — patrimônio e carteira em
  `meta.mes_carteira`, orçamento em `meta.mes_ref`.
- **`total_diversos` é categoria residual.** Alta em `diversos` é perda de
  granularidade, não necessariamente descontrole — trate como tal.
- **`transporte` tem sazonalidade forte** (IPVA, seguro, licenciamento
  concentram-se em poucos meses). Compare contra o mesmo mês do ano anterior
  antes de chamar de aumento.
- **Mês com data especial** (`meta.motivos_especiais` não nulo, ex.: aniversário
  de casamento) costuma elevar `role` e `diversos`. Cite o motivo em vez de
  tratar o pico como desvio de conduta.
- **`saude` e `educacao` não entram em sugestão de corte**, por decisão
  registrada no glossário.
- **`camada = 'NAO CLASSIFICADO'`** não é alocação: é pendência operacional.
  Vai para a lista de ações. Vale para saldo em conta também — o default de
  qualquer posição nova, inclusive das disponibilidades, é `NAO CLASSIFICADO`.
- **`RESERVA ESTRATEGICA` não tem alvo** (cripto, moeda estrangeira, cashback).
  Ela e `NAO CLASSIFICADO` ficam **fora do denominador** da alocação por camada:
  os alvos de 30/50/20 são sobre a carteira sem as duas. O montador já faz essa
  conta e imprime a nota das duas bases — ao citar percentual de camada na
  narrativa, use o da tabela, não o `pct_da_carteira` cru do JSON.
- **`riqueza.comparativo_*`** usa `RICO`/`POBRE` como rótulo interno de
  superação de benchmark. Não reproduza esses termos no PDF — escreva "acima do
  CDI" / "abaixo do IPCA".

## Passo 3 — Analisar

Trabalhe as duas frentes com o rigor de cada profissão.

**Planejador financeiro** (só Lucas e Jéssica):
- Taxa de poupança do mês e dos 12 meses, contra a meta da política.
- Composição da despesa por categoria: participação, variação contra a média
  móvel de 6 meses e contra o mesmo mês do ano anterior quando houver base.
- Separe fixo de variável e essencial de discricionário conforme o glossário —
  é isso que torna a recomendação acionável.
- Cobertura da reserva de emergência, em meses de despesa, contra a reserva-alvo
  da política: o maior entre N meses da **mediana** da despesa dos últimos 6
  meses fechados e o piso de R$ 100.000. Diga qual das duas regras está valendo.
- Conta de luz: separe efeito preço (`preco_kwh`) de efeito consumo (`kwh_dia`).

**Assessor de investimentos** (todos):
- Alocação atual por camada contra a alocação-alvo, com o desvio em pontos
  percentuais e a banda de tolerância da política.
- Concentração por instituição, emissor e conglomerado; exposição em moeda.
- Exposição FGC: conglomerados sem folga contra o limite.
- Vencimentos nos próximos 12 meses e o que fazer com o principal que retorna.
- Desempenho contra CDI e contra a inflação pessoal, separadamente.
- **Destino do aporte do mês**: qual camada e por quê, em valor.

Regras de conduta:
- Rebalanceie por aporte, não por venda, salvo desvio acima de 15 p.p.
- Nunca recomende produto ou emissor específico que já não esteja na carteira
  ou não seja instrumento genérico da camada (ex.: "Tesouro Selic" tudo bem;
  "CDB do banco X a 112% do CDI" não).
- Quando os dados não sustentarem uma conclusão, diga que não sustentam.

## Passo 4 — Escrever a narrativa

**Você não escreve HTML.** `scripts/montar_relatorio.py` renderiza KPIs, tabelas
e gráficos direto do JSON, para que nenhum número do PDF dependa de transcrição
sua. O que você escreve é o texto analítico, num arquivo `narrativa.json` por
escopo, gravado no scratchpad:

```json
{
  "sumario": "<p>…</p>",
  "diagnostico_orcamento": "<p>…</p>",
  "diagnostico_carteira": "<p>…</p>",
  "diagnostico_riscos": "<p>…</p>",
  "diagnostico_desempenho": "<p>…</p>",
  "recomendacoes": [{"titulo": "…", "texto": "…"}],
  "premissas": ["…"]
}
```

Todas as chaves são opcionais e o conteúdo é HTML — use `<p>` e `<strong>`, nada
mais. No escopo `deusa`, `diagnostico_orcamento` e `diagnostico_desempenho` não
são renderizados; não os escreva.

Sobre o texto:

- Números citados na narrativa têm que bater com os que o montador renderiza.
  Confira contra o JSON antes de escrever, não de memória.
- No máximo 6 recomendações, cada uma com valor em R$ quando couber e a razão
  em uma frase. Recomendação sem número é opinião.
- Liste em `premissas` os critérios que sustentaram alguma conclusão e que o
  leitor não deduziria dos números: qual das duas regras de reserva-alvo está
  valendo, qualquer marcação `[CONFIRMAR]` do glossário que você tenha usado,
  e as limitações do dado que afetaram o diagnóstico. Saem no rodapé do PDF.
- Quando os dados não sustentarem uma conclusão, escreva que não sustentam. O
  índice de `riqueza`, por exemplo, compõe variação de patrimônio que **inclui
  aportes** — não é rentabilidade, e compará-lo ao CDI superestima o desempenho
  da carteira. Diga isso em vez de omitir.

### O que o montador já produz sozinho

Não peça para escrever, não duplique na narrativa:

| # | Seção | Escopo |
|---|---|---|
| 1 | Capa, com aviso de defasagem entre carteira e DRE | ambos |
| 2 | Sumário executivo — 4 KPIs + `sumario` | ambos |
| 3 | Patrimônio — evolução, variação MoM, composição por titular | casal |
| 4 | Resultado do mês — receita, despesa, poupança + `diagnostico_orcamento` | casal |
| 5 | Gastos por categoria — variação vs. média 6m, conta de luz | casal |
| 6 | Carteira — camada vs. alvo, instituição, tipo, moeda, posições + `diagnostico_carteira` | ambos |
| 7 | Renda passiva — dividendos e yield | ambos |
| 8 | Riscos — FGC, vencimentos, não classificados + `diagnostico_riscos` | ambos |
| 9 | Desempenho contra CDI e inflação pessoal + `diagnostico_desempenho` | casal |
| 10 | Recomendações — a partir de `recomendacoes[]` | ambos |
| 11 | Glossário de categorias e camadas | ambos |
| 12 | Notas e procedência, com as `premissas[]` | ambos |

O glossário do PDF sai do próprio montador (`TEXTO_CATEGORIA` e `TEXTO_CAMADA`).
Se você mudar `_docs_financas.md`, **atualize esses dicionários junto** — são a
versão resumida do mesmo conteúdo e não podem divergir.

O montador já cuida de: paleta da `dataviz` sem alteração (camadas nos slots
1–3, categorias nos 8 na ordem declarada), cor por entidade e não por ranking,
eixo único, rótulo direto mais tabela irmã em todo gráfico, separação de 2px
entre fatias, e formatação pt-BR de moeda, percentual e data.

Só carregue a skill `dataviz` se precisar **acrescentar** um gráfico ao
montador. Para gerar um relatório com os cortes que já existem, não precisa.

## Passo 5 — Montar e converter

```bash
S=<scratchpad>
D=${RELATORIOS_DIR:-relatorios/<AAAA-MM>}

for escopo in casal deusa; do
  case $escopo in
    casal) nome=relatorio_lucas_jessica_<AAAA-MM> ;;
    deusa) nome=relatorio_deusa_<AAAA-MM> ;;
  esac
  python3 .claude/skills/relatorio-financas/scripts/montar_relatorio.py \
      --dados "$S/dados.json" --escopo "$escopo" \
      --narrativa "$S/narrativa_$escopo.json" --saida "$D/$nome.html"
  .claude/skills/relatorio-financas/scripts/html_para_pdf.sh \
      "$D/$nome.html" "$D/$nome.pdf"
done
```

Destino padrão: `relatorios/AAAA-MM/` na raiz do projeto (fora do git — já está
no `.gitignore`). Respeite `RELATORIOS_DIR` se estiver definida.

## Passo 6 — Conferir antes de entregar

Obrigatório, não opcional:

1. Converta a primeira página de cada PDF em imagem e **olhe**:
   ```bash
   pdftoppm -png -r 80 -f 1 -l 3 <arquivo.pdf> <prefixo>
   ```
   Leia as imagens. Procure texto cortado, tabela estourando a margem, gráfico
   sobreposto, página em branco, rótulo colidindo.
2. Confira dois ou três números **da sua narrativa** contra as tabelas que o
   montador renderizou. É o único ponto do fluxo onde um número pode divergir.
3. Verifique que nenhum mês futuro entrou em número ou gráfico.
4. Confirme que ambos os PDFs têm mais de uma página e tamanho plausível.

Se o texto estiver errado, corrija o `narrativa.json`; se o layout ou um número
renderizado estiver errado, corrija `montar_relatorio.py` ou `relatorio.css`.
Nos dois casos, monte e converta de novo. Não entregue um PDF que você não olhou.

## Encerramento

Informe os caminhos dos dois PDFs, o mês de referência de cada bloco, e liste
separadamente as pendências que apareceram — ativos não classificados,
conglomerados sem folga FGC, premissas `[CONFIRMAR]` que sustentaram alguma
recomendação.

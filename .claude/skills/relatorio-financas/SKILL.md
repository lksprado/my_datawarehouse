---
name: relatorio-financas
description: Varre a camada marts do domínio finanças e gera quatro relatórios mensais em PDF — um de investimentos para cada titular (Lucas, Jéssica e Deusa) e um de orçamento do casal (receita, despesa, gastos por categoria e reserva). Use quando o usuário pedir o relatório financeiro do mês, o fechamento mensal, ou o PDF de finanças.
---

# Relatório financeiro mensal

Produz quatro PDFs no padrão de um planejador financeiro pessoal somado a um
assessor de investimentos: diagnóstico do mês, leitura da carteira e destino
recomendado para o aporte.

| Relatório | Escopo | Conteúdo |
|---|---|---|
| `relatorio_investimentos_lucas_AAAA-MM.pdf` | `lucas` | Patrimônio, carteira, renda passiva, riscos, desempenho vs. benchmark, aporte |
| `relatorio_investimentos_jessica_AAAA-MM.pdf` | `jessica` | idem |
| `relatorio_investimentos_deusa_AAAA-MM.pdf` | `deusa` | Carteira, renda passiva, riscos, aporte |
| `relatorio_orcamento_casal_AAAA-MM.pdf` | `orcamento` | Receita, despesa, resultado, poupança, gastos por categoria, luz, reserva de emergência |

A separação é por assunto **e** por titular: investimento é individual, orçamento
é do casal. Consequências que valem para o diagnóstico:

- **Não há lançamento de despesa por pessoa.** Receita e despesa são do casal, e
  por isso existem em um relatório só. Nenhum dos três relatórios de investimento
  fala de gasto. Não invente despesa individual, nem para Deusa.
- **A reserva de emergência é do casal** e mora no relatório de orçamento, porque
  é dimensionada pela mediana da despesa. Nos individuais a reserva aparece só
  como camada contra o alvo de alocação.

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
recomendação saem de lá.

Cada categoria de gasto termina com uma subseção **Fatos relevantes**: eventos
datados que explicam variação brusca e não existem em nenhum campo do dado, uns
já realizados e outros só previstos. É o que separa evento planejado de
descontrole — leia antes de julgar um desvio de categoria e antes de ler queda
de patrimônio como desempenho ruim. Fato usado no diagnóstico entra em
`premissas`, com o status (previsto ou realizado). Se um parâmetro estiver marcado `[CONFIRMAR]`, use-o
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

Leia `meta.prontidao`. São **dois portões independentes**, porque as duas
famílias de relatório dependem de dados diferentes:

| flag | o que exige | bloqueia |
|---|---|---|
| `pronto_orcamento` | DRE presente, com variáveis, lançado até o fim do mês | `relatorio_orcamento_casal` |
| `pronto_investimentos` | carteira no máximo 2 meses atrás do mês de referência | os três `relatorio_investimentos_*` |

`pronto` é o E dos dois e serve só como resumo. **Um pode passar sem o outro** —
mês de gasto ainda carregando não segura mais a leitura de carteira, e carteira
defasada não segura o orçamento.

Se um portão reprovar, **não gere os relatórios daquela família**: relate as
`pendencias_orcamento` / `pendencias_investimentos` em português claro, diga
qual foi o último mês fechado (`ultimo_mes_fechado`) e ofereça gerar aquele.
Gere normalmente a família que passou, e diga ao usuário o que ficou de fora.
Não decida sozinho gerar o que foi reprovado.

O portão do orçamento distingue três situações que, nos números, se parecem:

| situação | como aparece |
|---|---|
| mês fechado | 26–28 dias com gasto, lançamento até o último dia |
| mês em andamento | lançamentos param no dia de hoje |
| mês pré-lançado | só as fixas (dia 25), `role`/`diversos`/`transporte` zerados |

Se o usuário mandar gerar mesmo assim, siga — o montador imprime a tarja de
dados incompletos na capa e no rodapé automaticamente, no escopo certo, e o
diagnóstico precisa dizer que os valores estão parciais.

### Armadilhas dos dados — leia antes de interpretar

- **`marts.resultado` e `marts.consumo` contêm meses futuros pré-lançados** com
  as despesas fixas recorrentes já agendadas. Não são realizados. A extração já
  corta em `mes_ref`; nunca reintroduza meses posteriores no diagnóstico nem em
  médias.
- **A carteira fecha em cadência própria** e costuma estar 1 mês atrás do DRE.
  `meta.defasagem_carteira_meses` diz o quanto. Quando for maior que zero,
  declare no relatório a data de cada número — patrimônio e carteira em
  `meta.mes_carteira`, orçamento em `meta.mes_ref`. No relatório de orçamento a
  reserva de emergência é o único número que vem da carteira, e é onde a
  defasagem aparece.
- **`total_diversos` é categoria residual.** Alta em `diversos` é abuso de compras
  desnecessárias - categoria ideal para redução de custos.
- **`transporte` tem sazonalidade forte** (IPVA, seguro, licenciamento
  concentram-se em poucos meses). Compare contra o mesmo mês do ano anterior
  antes de chamar de aumento.
- **Mês dentro do período de um fato relevante** (subseção da categoria no
  glossário): o desvio é execução de plano até a ordem de grandeza declarada; o
  excedente acima dela é que precisa de explicação própria. Nada é removido de
  média, mediana ou reserva-alvo por causa de um fato — muda a leitura, não o
  número.
- **Mês com data especial** (`meta.motivos_especiais` não nulo, ex.: aniversário
  de casamento) costuma elevar `role` e `diversos` por causa de viagens ou presentes.
  Cite o motivo em vez de tratar o pico como desvio de conduta.
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

Trabalhe as duas frentes com o rigor de cada profissão. Cada frente vira um tipo
de relatório: o planejador escreve o de orçamento, o assessor escreve os três de
investimento.

**Planejador financeiro** (escopo `orcamento`, o casal):
- Taxa de poupança do mês e dos 12 meses, contra a meta da política.
- Composição da despesa por categoria: participação, variação contra a média
  móvel de 6 meses e contra o mesmo mês do ano anterior quando houver base.
- Separe fixo de variável e essencial de discricionário conforme o glossário —
  é isso que torna a recomendação acionável.
- Cobertura da reserva de emergência, em meses de despesa, contra a reserva-alvo
  da política: o maior entre N meses da **mediana** da despesa dos últimos 6
  meses fechados e o piso de R$ 100.000. Diga qual das duas regras está valendo.
- Conta de luz: separe efeito preço (`preco_kwh`) de efeito consumo (`kwh_dia`).

**Assessor de investimentos** (um relatório por titular — a análise é da carteira
daquela pessoa, contra a alocação-alvo dela):
- Alocação atual por camada contra a alocação-alvo, com o desvio em pontos
  percentuais e a banda de tolerância da política.
- Concentração por instituição, emissor e conglomerado; exposição em moeda.
- Exposição FGC: conglomerados sem folga contra o limite.
- Vencimentos nos próximos 12 meses e o que fazer com o principal que retorna.
- Desempenho contra CDI e contra a inflação pessoal, separadamente (Lucas e
  Jéssica; Deusa não tem série).
- **Destino do aporte do mês**: qual camada e por quê, em valor.

Os alvos são por pessoa e diferentes entre si — 30/50/20 para Lucas, 30/70/0
para Jéssica, 30/60/10 para Deusa. Não transporte a leitura de uma carteira
para a outra.

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

São quatro arquivos: `narrativa_orcamento.json`, `narrativa_lucas.json`,
`narrativa_jessica.json`, `narrativa_deusa.json`. Todas as chaves são opcionais e
o conteúdo é HTML — use `<p>` e `<strong>`, nada mais. **Chave que o escopo não
renderiza é trabalho jogado fora**; não a escreva:

| escopo | chaves renderizadas |
|---|---|
| `orcamento` | `sumario`, `diagnostico_orcamento`, `recomendacoes`, `premissas` |
| `lucas`, `jessica` | `sumario`, `diagnostico_carteira`, `diagnostico_riscos`, `diagnostico_desempenho`, `recomendacoes`, `premissas` |
| `deusa` | idem, sem `diagnostico_desempenho` |

As `recomendacoes` do orçamento são sobre gasto, poupança e reforço da reserva;
as dos três individuais são sobre o destino do aporte por camada. Não repita a
mesma recomendação nos dois lugares.

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

**Escopo `orcamento`:**

| # | Seção |
|---|---|
| — | Capa, com a nota de que a reserva vem da carteira e não do mês do orçamento |
| 1 | Sumário executivo — receita, resultado, despesa, cobertura da reserva + `sumario` |
| 2 | Resultado do mês — receita, despesa, poupança, 13 meses + `diagnostico_orcamento` |
| 3 | Gastos por categoria — variação vs. média 6m, conta de luz |
| 4 | Reserva de emergência — reserva por titular, cobertura, alvo e qual regra vale |
| 5 | Recomendações — a partir de `recomendacoes[]` |
| 6 | Glossário de categorias de gasto |
| — | Notas e procedência, com as `premissas[]` |

**Escopos `lucas` / `jessica` / `deusa`** (a numeração é sequencial; no relatório
de Deusa as seções de patrimônio e desempenho não existem e as demais sobem):

| # | Seção | Quem |
|---|---|---|
| — | Capa | todos |
| 1 | Sumário executivo — 4 KPIs + `sumario` | todos |
| 2 | Patrimônio — evolução do titular, variação mensal, % do casal | lucas, jessica |
| 3 | Carteira — camada vs. alvo, instituição, tipo, moeda, posições + `diagnostico_carteira` | todos |
| 4 | Renda passiva — dividendos e yield | todos |
| 5 | Riscos — FGC, vencimentos, não classificados + `diagnostico_riscos` | todos |
| 6 | Desempenho contra CDI e inflação pessoal + `diagnostico_desempenho` | lucas, jessica |
| 7 | Recomendações — a partir de `recomendacoes[]` | todos |
| 8 | Glossário de camadas | todos |
| — | Notas e procedência, com as `premissas[]` | todos |

O glossário do PDF sai do próprio montador (`TEXTO_CATEGORIA` no orçamento,
`TEXTO_CAMADA` nos individuais).
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

for escopo in orcamento lucas jessica deusa; do
  case $escopo in
    orcamento) nome=relatorio_orcamento_casal_<AAAA-MM> ;;
    *)         nome=relatorio_investimentos_${escopo}_<AAAA-MM> ;;
  esac
  python3 .claude/skills/relatorio-financas/scripts/montar_relatorio.py \
      --dados "$S/dados.json" --escopo "$escopo" \
      --narrativa "$S/narrativa_$escopo.json" --saida "$D/$nome.html"
  .claude/skills/relatorio-financas/scripts/html_para_pdf.sh \
      "$D/$nome.html" "$D/$nome.pdf"
done
```

Gere só os escopos cujo portão passou (Passo 2). Se o portão do orçamento
reprovou, o laço é `for escopo in lucas jessica deusa`.

Destino padrão: `relatorios/AAAA-MM/` na raiz do projeto (fora do git — já está
no `.gitignore`). Respeite `RELATORIOS_DIR` se estiver definida.

## Passo 6 — Conferir antes de entregar

Obrigatório, não opcional:

1. Converta as primeiras páginas de cada PDF em imagem e **olhe**:
   ```bash
   pdftoppm -png -r 80 -f 1 -l 3 <arquivo.pdf> <prefixo>
   ```
   Leia as imagens. Procure texto cortado, tabela estourando a margem, gráfico
   sobreposto, página em branco, rótulo colidindo.
2. Confira dois ou três números **da sua narrativa** contra as tabelas que o
   montador renderizou. É o único ponto do fluxo onde um número pode divergir.
3. Verifique que nenhum mês futuro entrou em número ou gráfico.
4. Confirme que cada PDF gerado tem mais de uma página e tamanho plausível.
5. Confira que o assunto não vazou de um relatório para o outro: nenhum dos
   individuais fala de despesa, e o de orçamento não traz posição de carteira.
   No de Deusa, a numeração das seções não pode ter buraco.

Se o texto estiver errado, corrija o `narrativa.json`; se o layout ou um número
renderizado estiver errado, corrija `montar_relatorio.py` ou `relatorio.css`.
Nos dois casos, monte e converta de novo. Não entregue um PDF que você não olhou.

## Encerramento

Informe os caminhos dos PDFs gerados, o mês de referência de cada bloco, e diga
explicitamente se algum escopo ficou de fora por causa do portão. Liste
separadamente as pendências que apareceram — ativos não classificados,
conglomerados sem folga FGC, premissas `[CONFIRMAR]` que sustentaram alguma
recomendação — dizendo de qual titular é cada uma.

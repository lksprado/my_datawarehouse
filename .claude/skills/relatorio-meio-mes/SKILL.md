---
name: relatorio-meio-mes
description: Gera o relatório de acompanhamento do mês EM ANDAMENTO — ritmo do gasto até hoje, projeção de fechamento, quanto ainda cabe gastar por categoria, mais o desempenho do patrimônio contra CDI e inflação pessoal do mês anterior. Use quando o usuário pedir o acompanhamento de meio de mês, como está o gasto do mês, se vai estourar o orçamento, ou o desempenho contra os benchmarks. NÃO é o fechamento mensal — para os quatro PDFs do mês fechado, use relatorio-financas.
---

# Relatório de meio de mês

Um PDF, do casal: `relatorio_meio_mes_AAAA-MM.pdf`, onde AAAA-MM é o **mês
corrente**. Roda entre os dias 15 e 20.

## Por que existe

As fontes do domínio não ficam prontas ao mesmo tempo, e o relatório de
fechamento roda cedo demais para duas coisas:

| Fonte | Quando fica confiável |
|---|---|
| `marts.consumo` (gasto diário) | contínuo, ~D+1 |
| `marts.resultado` (DRE) | primeiros dias do mês seguinte |
| carteira e patrimônio | fecham em cadência própria, costumam vir 1 mês atrás |
| indexadores IPCA/CDI/Selic/inflação pessoal | **IPCA sai ~dia 10**; a planilha é preenchida depois |

Daí as duas partes, com recortes de tempo diferentes:

1. **Mês corrente** (partes 1 a 4) — o gasto diário é o dado mais fresco do
   domínio e, no fechamento, era lido uma vez por mês, quando já não dava mais
   para agir. No dia 17 metade do mês passou e a outra metade ainda é decisão.
2. **Mês anterior** (parte 5) — o desempenho contra benchmark, que saiu do
   relatório de fechamento porque lá os indexadores ainda não existem. Isso
   fechava a série um mês antes, em silêncio.

**O que este relatório não faz:** posição de carteira, alocação por camada,
exposição ao FGC, vencimentos, reserva de emergência, destino do aporte. Tudo
isso é do `relatorio-financas`. Não repita nem antecipe.

**Acionamento: sob demanda.** Não há agendador. Para rodar sem sessão
interativa existe `scripts/gerar_relatorio_meio_mes.sh`.

## Passo 0 — Ler as regras do domínio

**Antes de qualquer análise**, leia `models/marts/financas/_docs_financas.md`.

Aqui as subseções **Fatos relevantes** de cada categoria valem mais do que no
fechamento, porque este relatório dispara alerta sobre mês em curso. Um pico
previsto não é descontrole: em agosto/2026 há troca de carro (~R$ 15.000 em
`transporte`) e a partir de setembro o transplante capilar (~R$ 25.000 em
`saude`). Ler o desvio sem ler o fato produz uma recomendação que manda cortar
o que já estava decidido. Fato usado no diagnóstico entra em `premissas`, com o
status (previsto ou realizado).

Os parâmetros numéricos vêm de `scripts/relatorios/politica.py`, cópia única
compartilhada com o `relatorio-financas`. Se ela e o Markdown discordarem,
**vale o `_docs_financas.md`** — e corrija o Python na mesma passada.

## Passo 1 — Resolver a data

```bash
date +%Y-%m-%d
```

**Rode o comando. Não use a data que você acha que é.**

- O mês de referência é o **corrente**, e o corte é hoje. Isto é o oposto do
  relatório de fechamento, que nunca fala do mês corrente — não confunda os
  dois.
- Se o usuário pedir uma data específica ("como estava dia 17 de junho"), use
  a data dele. A extração é parametrizada por data justamente para isso.
- Antes do dia 10 o portão reprova. Diga isso e ofereça rodar mais tarde, em
  vez de gerar um relatório que o próprio dado não sustenta.

Declare a data resolvida antes de extrair.

## Passo 2 — Extrair os dados

```bash
.claude/skills/relatorio-meio-mes/scripts/extrair_dados.sh <AAAA-MM-DD> <saida.json>
```

Grave o JSON no scratchpad da sessão, não no repositório.

**Todo número do relatório sai desse JSON.** Não consulte o banco por fora nem
recalcule agregados de cabeça — se faltar um corte, acrescente o bloco em
`queries/extrair_meio_mes.sql` e rode de novo.

### Portão de prontidão

Leia `meta.prontidao`. São **dois portões independentes**:

| flag | o que exige | bloqueia |
|---|---|---|
| `pronto_ritmo` | hoje ≥ dia 10; último lançamento a ≤ 3 dias; ≥ 7 dias com gasto | partes 1 a 4 |
| `pronto_indicadores` | `marts.indicadores` tem o mês anterior com `ipca` preenchido | parte 5 |

Um passa sem o outro. Se `pronto_indicadores` reprovar, o montador **omite** a
seção de desempenho e renumera sozinho — não force. Se `pronto_ritmo` reprovar,
o relatório inteiro perde o sentido: relate as `pendencias_ritmo` em português
claro e não gere, a menos que o usuário mande.

`meta.meses_de_base` diz quantos meses fechados sustentam a mediana. Menos de 6
é pendência declarada, e precisa ir para `premissas`.

### Armadilhas dos dados — leia antes de interpretar

- **O eixo é `dia_fatura`, não o dia do calendário.** É o `dia_ajustado` da
  planilha, o dia deslocado para a posição que ocupa dentro do ciclo de fatura.
  É o que torna dois meses comparáveis dia a dia. Ao escrever "até o dia 17",
  entenda dia 17 *do ciclo*.
- **Linhas do mês corrente com data futura são fixas pré-agendadas**, não gasto
  realizado. Saem em `agendado` e entram só na projeção. As fixas caem no dia
  25: no dia 17 elas ainda não aconteceram, e o acumulado estar abaixo da
  mediana por causa disso **não** é economia.
- **A projeção é conservadora por construção**:
  `realizado + GREATEST(mediana do que cai após o corte, o já agendado)`. É o
  maior dos dois e não a soma, porque a mediana histórica já embute as fixas —
  somar contaria duas vezes. Declare a regra em `premissas`.
- **A coluna de projeção não soma ao total, e isso está certo.** A mediana de
  uma soma não é a soma das medianas. O montador já imprime a nota e o tamanho
  da diferença. Para julgar o mês inteiro use o total; para julgar uma
  categoria use a linha dela. Nunca reconcilie os dois na narrativa.
- **`transporte` tem sazonalidade forte** (IPVA, seguro, licenciamento).
  Compare contra o mesmo mês do ano anterior antes de chamar de aumento.
- **`total_diversos` é categoria residual.** Alta em `diversos` é abuso de
  compras desnecessárias — a categoria mais indicada para redução.
- **`saude` e `educacao` não entram em sugestão de corte**, por decisão
  registrada no glossário. Elas nem aparecem na seção de margem.
- **A receita do mês corrente já está lançada** (é o salário) e por isso a
  poupança projetada é confiável do lado da receita. A despesa é projeção.
- **O índice de `riqueza` inclui aportes** — não é rentabilidade, e compará-lo
  ao CDI superestima o desempenho da carteira. Responde "o patrimônio cresceu
  mais que a inflação?", não "a carteira bateu o CDI?". Diga isso em vez de
  omitir.
- **`riqueza.comparativo_*`** usa `RICO`/`POBRE` como rótulo interno. Não
  reproduza esses termos no PDF — escreva "acima do CDI" / "abaixo do IPCA".

## Passo 3 — Analisar

Você é o planejador financeiro do casal olhando um mês que ainda dá para
mudar. A pergunta não é "como foi", é **"o que fazer nos dias que restam"**.

- **Ritmo**: o acumulado está dentro da faixa dos seis meses fechados no mesmo
  dia do ciclo? Se saiu, quando saiu e por quê.
- **Categorias**: quais projeções destoam da mediana do mês cheio, e o quanto
  disso já está comprometido (`agendado`) contra o quanto ainda é decisão.
- **Margem**: quanto ainda cabe em `role`, `diversos` e `mercado` para fechar
  dentro do padrão e para bater a meta de poupança. É a seção que justifica o
  relatório existir no dia 17 e não no dia 30 — priorize-a.
- **Desempenho** (só se `pronto_indicadores`): patrimônio contra CDI e contra a
  inflação pessoal, separadamente, sempre com a ressalva de que o índice inclui
  aportes.

Regras de conduta:
- Recomendação sem número é opinião. Diga o valor em reais e o prazo.
- Não recomende cortar o que já está comprometido — `agendado` não é decisão.
- Quando os dados não sustentarem uma conclusão, diga que não sustentam.

## Passo 4 — Escrever a narrativa

**Você não escreve HTML.** `scripts/montar_meio_mes.py` renderiza KPIs, tabelas
e gráficos direto do JSON. Você escreve `narrativa_meio_mes.json` no scratchpad:

```json
{
  "sumario": "<p>…</p>",
  "diagnostico_ritmo": "<p>…</p>",
  "diagnostico_categorias": "<p>…</p>",
  "diagnostico_desempenho": "<p>…</p>",
  "recomendacoes": [{"titulo": "…", "texto": "…"}],
  "premissas": ["…"]
}
```

Todas as chaves são opcionais; o conteúdo é HTML restrito a `<p>` e `<strong>`.
`diagnostico_desempenho` só é renderizada se `pronto_indicadores` — se o portão
reprovou, não a escreva.

Sobre o texto:

- Números citados têm que bater com os que o montador renderiza. Confira contra
  o JSON, não de memória.
- No máximo 6 recomendações, cada uma com valor em R$ e prazo dentro do mês.
- Em `premissas`, liste: a regra da projeção, quantos meses fechados
  sustentaram a mediana, qualquer `[CONFIRMAR]` do glossário que você usou, e
  os fatos relevantes que explicaram um desvio.

### O que o montador já produz sozinho

Não peça para escrever, não duplique na narrativa:

| # | Seção | Conteúdo |
|---|---|---|
| — | Capa | Com a nota de que a última seção fala do mês anterior |
| 1 | Sumário — gasto até o corte, projeção, poupança projetada, dias restantes + `sumario` |
| 2 | Ritmo do mês — acumulado diário contra faixa e mediana + `diagnostico_ritmo` |
| 3 | Categorias — realizado, agendado, projeção, desvio + `diagnostico_categorias` |
| 4 | Margem disponível — teto de despesa e folga por categoria comprimível |
| 5 | Desempenho do mês anterior + `diagnostico_desempenho` — **só se o portão passou** |
| 6 | Recomendações — a partir de `recomendacoes[]` |
| 7 | Glossário de categorias de gasto |
| — | Notas e procedência, com as `premissas[]` |

A numeração é sequencial: sem a seção 5, as seguintes sobem. Não pode haver
buraco.

## Passo 5 — Montar e converter

```bash
S=<scratchpad>
D=${RELATORIOS_DIR:-relatorios/<AAAA-MM>}
nome=relatorio_meio_mes_<AAAA-MM>

python3 .claude/skills/relatorio-meio-mes/scripts/montar_meio_mes.py \
    --dados "$S/dados.json" --narrativa "$S/narrativa_meio_mes.json" \
    --saida "$D/$nome.html"
.claude/skills/relatorio-meio-mes/scripts/html_para_pdf.sh \
    "$D/$nome.html" "$D/$nome.pdf"
```

Destino padrão: `relatorios/AAAA-MM/` na raiz (fora do git). Respeite
`RELATORIOS_DIR` se estiver definida.

## Passo 6 — Conferir antes de entregar

Obrigatório, não opcional:

1. Converta as páginas em imagem e **olhe**:
   ```bash
   pdftoppm -png -r 80 -f 1 -l 4 <arquivo.pdf> <prefixo>
   ```
   Procure texto cortado, tabela estourando a margem, gráfico sobreposto,
   rótulo colidindo, linha de tabela quebrando em duas.
2. Confira dois ou três números da sua narrativa contra as tabelas renderizadas.
3. **Nenhum número das partes 1 a 4 pode ser do mês anterior, e nenhum da parte
   5 pode ser do mês corrente.** É o erro mais provável deste relatório.
4. Verifique que nenhum mês futuro entrou em número ou gráfico — setembro
   existe na base como pré-lançado.
5. Confirme que o relatório não fala de carteira, camada, FGC, vencimento nem
   reserva de emergência. Isso é do fechamento.
6. A numeração das seções não pode ter buraco.

Se o texto estiver errado, corrija a narrativa; se o layout ou um número
renderizado estiver errado, corrija `montar_meio_mes.py` ou
`scripts/relatorios/relatorio.css`. **Mexer em `scripts/relatorios/` afeta
também o relatório de fechamento** — confira os dois. Nos dois casos, monte e
converta de novo. Não entregue um PDF que você não olhou.

## Encerramento

Informe o caminho do PDF, a data de corte, o mês de cada bloco, e diga
explicitamente se a seção de desempenho ficou de fora por causa do portão.
Liste as pendências que apareceram — categorias já estouradas, poupança
projetada abaixo da meta, base de comparação com menos de 6 meses, premissas
`[CONFIRMAR]` que sustentaram alguma recomendação.

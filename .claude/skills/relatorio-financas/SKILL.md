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

## Passo 0 — Ler as regras do domínio

**Antes de qualquer análise**, leia `models/marts/financas/_docs_financas.md`.

É a fonte única do que cada categoria de gasto engloba, do que cada camada de
investimento significa e da política de investimento (aporte alvo, alocação-alvo
por camada, metas de reserva, limites e benchmark). Todo diagnóstico e toda
recomendação saem de lá. Se um parâmetro estiver marcado `[CONFIRMAR]`, use-o
mesmo assim e sinalize no relatório que é premissa a validar — não o substitua
por um número inventado.

## Passo 1 — Extrair os dados

```bash
.claude/skills/relatorio-financas/scripts/extrair_dados.sh <AAAA-MM> <saida.json>
```

Sem argumento de mês, usa o mês anterior ao atual. Grave o JSON no diretório de
scratchpad da sessão, não no repositório.

**Todo número dos relatórios sai desse JSON.** Não consulte o banco por fora nem
recalcule agregados de cabeça — se faltar um corte, acrescente o bloco em
`queries/extrair.sql` e rode de novo.

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
- **`saude` e `educacao` não entram em sugestão de corte**, por decisão
  registrada no glossário.
- **`camada = 'NAO CLASSIFICADO'`** não é alocação: é pendência operacional.
  Vai para a lista de ações, e o percentual de cada camada deve ser reportado
  com nota de que há valor não classificado fora da conta.
- **`riqueza.comparativo_*`** usa `RICO`/`POBRE` como rótulo interno de
  superação de benchmark. Não reproduza esses termos no PDF — escreva "acima do
  CDI" / "abaixo do IPCA".

## Passo 2 — Analisar

Trabalhe as duas frentes com o rigor de cada profissão.

**Planejador financeiro** (só Lucas e Jéssica):
- Taxa de poupança do mês e dos 12 meses, contra a meta da política.
- Composição da despesa por categoria: participação, variação contra a média
  móvel de 6 meses e contra o mesmo mês do ano anterior quando houver base.
- Separe fixo de variável e essencial de discricionário conforme o glossário —
  é isso que torna a recomendação acionável.
- Cobertura da reserva de emergência em meses de despesa, contra a meta.
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

## Passo 3 — Escrever a narrativa

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
- Liste em `premissas` toda marcação `[CONFIRMAR]` do glossário que sustentou
  alguma conclusão. Elas saem no rodapé do PDF.
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

## Passo 4 — Montar e converter

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

## Passo 5 — Conferir antes de entregar

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

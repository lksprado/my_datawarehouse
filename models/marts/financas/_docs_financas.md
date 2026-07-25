{#
  ============================================================================
  Dicionário semântico do domínio FINANÇAS.

  Este arquivo é a FONTE ÚNICA de verdade sobre:
    1. o que cada categoria de gasto engloba;
    2. o que significa cada camada de investimento;
    3. a política de investimento (aporte, alocação-alvo, metas e limites).
    4. demais conhecimentos que auxiliem calcular e interpretar os dados

  É lido em dois lugares:
    - pelo `dbt docs generate` (os blocos `{% docs %}` abaixo são referenciados
      em `_schema.yml` via `{{ doc('...') }}`);
    - pela skill `/relatorio-financas`, que usa estas definições para escrever
      o diagnóstico e as recomendações dos relatórios em PDF.

  Ao mudar uma regra de classificação de gasto ou de camada, edite AQUI —
  não no schema.yml e não na skill.

  ============================================================================
#}


{% docs categoria_mercado %}

**Mercado** — abastecimento da casa.

Entra: supermercado, hortifruti, feira, marmitas fit, açougue, peixaria, padaria (compra de despensa),
bebidas para consumo em casa, produtos de limpeza, higiene pessoal e itens de
uso doméstico não duráveis.

Não entra: refeição consumida fora de casa ou delivery (→ `role`); medicamento
e farmácia (→ `saude`); utensílios, móvel ou eletrodomésticos (→ `apartamento` / `diversos`).

Natureza: variável, essencial. É o principal item de despesa compressível do
orçamento — variações relevantes mês a mês costumam ser volume de compra, não
preço.

{% enddocs %}


{% docs categoria_diversos %}

**Diversos** — categoria residual.

Entra: vestuário e calçado, presentes, eletrônicos e acessórios pessoais, objetos domésticos, imprevistos e qualquer
lançamento que não caiba nas demais categorias.

Não entra: nada que tenha categoria própria. Se um tipo de gasto aparece em
`diversos` de forma recorrente, ele deixou de ser residual e merece categoria
nova.

Natureza: variável, majoritariamente discricionário. Flags `fl_data_especial`e `fl_mes_especial` podem sinalizar
picos desse gasto.

Sinal de alerta: `diversos` acima de ~20% da despesa total do mês indica perda
de granularidade — o relatório não consegue recomendar corte sobre uma
categoria que não diz o que é.

{% enddocs %}


{% docs categoria_assinaturas %}

**Assinaturas** — serviços recorrentes de cobrança automática.

Entra: streaming de vídeo e música, armazenamento em nuvem, licenças de
software, telefonia móvel, academia e clubes com mensalidade, jornais e
revistas, quaisquer serviços com renovação automática.

Não entra: internet fixa do apartamento (→ `apartamento`); plano de saúde
(→ `saude`); mensalidade de curso (→ `educacao`).

Natureza: fixa, discricionário na maior parte. É a categoria com maior razão
entre facilidade de corte e esforço — cancelamento é decisão única com efeito
permanente, ao contrário de `mercado` ou `role`, que exigem disciplina mensal.

{% enddocs %}


{% docs categoria_role %}

**Rolê** — lazer e consumo fora de casa.

Entra: bares e restaurantes, delivery e aplicativos de comida, cafés, cinema,
shows, eventos, viagens (hospedagem e passeios), compras de mercado exclusivamente para eventos, hobbies e lazer em geral.

Não entra: transporte usado para chegar ao rolê (→ `transporte`);

Natureza: variável, discricionário. É o item que mais responde a decisão
consciente no curto prazo e o primeiro a ser revisto quando o resultado do mês
fica abaixo da meta de poupança. Flags `fl_data_especial`e `fl_mes_especial` podem sinalizar
picos desse gasto.

{% enddocs %}


{% docs categoria_transporte %}

**Transporte** — deslocamento.

Entra: combustível, aplicativos de transporte, transporte público,
estacionamento, pedágio, manutenção e revisão do veículo, seguro do veículo,
IPVA e licenciamento.

Não entra: viagem de lazer com hospedagem (→ `role`).

Natureza: mista — combustível e aplicativos são variáveis; seguro, IPVA e
licenciamento são fixos anuais e concentram-se em poucos meses, o que distorce
a comparação mês a mês. O relatório deve tratar picos de `transporte` como
sazonalidade antes de tratá-los como descontrole.

{% enddocs %}


{% docs categoria_apartamento %}

**Apartamento** — moradia e sua manutenção.

Entra: condomínio, IPTU, energia elétrica, internet fixa, móveis,
reformas, reparos e serviços domésticos.

Não entra: produtos de limpeza e itens de consumo da casa (→ `mercado`).

Natureza: fixa e essencial em quase toda a sua composição — é o piso do
orçamento. Móveis e reformas são exceções: são investimentos pontuais em bem
durável e devem ser lidos separadamente da despesa corrente de moradia.

Referência cruzada: a conta de energia tem detalhamento próprio no mart `luz`
(valor, kWh, consumo diário e preço por kWh), útil para separar aumento de
tarifa de aumento de consumo.

{% enddocs %}


{% docs categoria_saude %}

**Saúde** — saúde física e mental.

Entra: plano de saúde, consultas, exames, procedimentos, odontologia, terapia,
farmácia e medicamentos.

Não entra: academia (→ `assinaturas`).

Natureza: mista — plano de saúde é fixo e essencial; o restante é variável e
não compressível. Nunca deve ser objeto de recomendação de corte: quando
`saude` sobe, o relatório reporta e explica, não sugere reduzir.

{% enddocs %}


{% docs categoria_educacao %}

**Educação** — formação e desenvolvimento.

Entra: cursos, certificações, livros, plataformas de ensino.

Não entra: assinatura de plataforma de conteúdo genérico (→ `assinaturas`);

Natureza: variável, discricionário no curto prazo mas com retorno esperado no
longo prazo. Deve ser tratado no relatório como investimento em capital humano,
não como consumo — não entra nas sugestões de corte por padrão.

{% enddocs %}

{% docs ajuste %}

**Ajuste Realizado** — Equilíbrio de despesas conjuntas.

Representa o valor transferido para o Lucas com o objetivo de equilibrar as despesas do casal de forma proporcional à renda de cada um.
Como o Lucas centraliza o pagamento das despesas compartilhadas, é realizada uma compensação mensal pela Jéssica para equalizar a participação de ambos nesses custos.
O valor da compensação já considera a dedução das despesas individuais do Lucas. Por ser um ajuste realizado posteriormente ao registro das receitas e despesas, ele não integra a composição dessas movimentações.
Os saldos das contas correntes já refletem essa compensação, uma vez que o valor é considerado como crédito para o Lucas e débito para a Jéssica.
Essa informação é apresentada apenas como referência para facilitar a análise financeira.


{% enddocs %}


{% docs camada_investimento %}


**Camada de alocação** — o papel que o ativo cumpre na carteira.

A camada não descreve o que o ativo é (isso é `tipo_investimento`), e sim para
que ele existe na carteira. É classificada manualmente por Lucas na aba
`classificacao` da planilha e volta ao warehouse via
`stg_carteira_classificacao`, com histórico SCD2 (join as-of por `mes_base`).

| Camada | Objetivo | Horizonte | Instrumentos típicos |
|---|---|---|---|
| `RESERVA` | Liquidez e preservação de capital | D+0 a D+1 | CDB de liquidez diária aplicado por conta remunerada, RDB com liquidez diária |
| `CRESCIMENTO` | Acumulação de patrimônio, aceita volatilidade | 2 anos ou mais | RDB de vencimento, CDB, LCA, LCI, ações, ETFs, BDRs, fundos de ações, cripto, prefixado e IPCA+ longos |
| `RENDA` | Geração de fluxo de caixa recorrente | 3 anos ou mais | FIIs, ações pagadores de dividendos |
| `NAO CLASSIFICADO` | — | — | Default automático a ser regularizado |

`RESERVA` — o critério é liquidez e ausência de marcação a mercado negativa, não
o rótulo do produto. Um CDB com vencimento em três anos e sem liquidez diária
não é reserva, ainda que seja renda fixa. O tamanho da reserva é definido em
meses de despesa (ver política abaixo), não em percentual da carteira: reserva
existe para cobrir despesa emergencial, não para acompanhar o patrimônio.

`CRESCIMENTO` — é a camada que aceita perda temporária em troca de retorno
esperado maior. Renda fixa longa entra aqui, e não em `RESERVA`, quando está
sujeita a marcação a mercado. Investimentos no exterior que rendem dividendos
também são considerados aqui.

`RENDA` — o que qualifica é a previsibilidade do fluxo, não o rendimento total.
Sobreposição com `CRESCIMENTO` é esperada (uma ação pagadora de dividendo também
se valoriza); a classificação segue a intenção de uso do ativo.

`NAO CLASSIFICADO` — não é uma camada, é ausência de classificação. Um ativo cai
aqui quando é novo na carteira ou quando sua chave de identidade mudou (`pessoa`,
`investimento`, `categoria_investimento`, `tipo_investimento`, `instituicao`).
Tickers de renda variável são estáveis; descrições de renda fixa podem mudar e
quebrar o vínculo. Todo `NAO CLASSIFICADO` no mês corrente é pendência
operacional e deve aparecer no relatório como item de ação, não como alocação.

{% enddocs %}


{% docs politica_investimentos %}

**Política de investimento** — parâmetros que o relatório usa para transformar
diagnóstico em recomendação.

Sem estes números, o relatório só consegue descrever a carteira. Com eles,
consegue dizer para onde vai o aporte do mês.

### Aporte mensal

| Pessoa | Aporte alvo (R$/mês) | Origem |
|---|---|---|
| Lucas | 4.000 | Resultado mensal (receita − despesa) |
| Jéssica | 2.500 | Resultado mensal (receita − despesa) |
| Deusa | 3.000 | Renda própria |

Regra de aporte: o valor efetivo do mês é o resultado apurado no mart
`resultado`, não o alvo. O alvo serve para medir aderência — quando o resultado
fica abaixo dele, o relatório aponta a categoria de despesa que explica a
diferença.

### Alocação-alvo por camada

| Pessoa | RESERVA | CRESCIMENTO | RENDA | Perfil |
|---|---|---|---|---|
| Lucas | 25% | 55% | 20% | Arrojado — horizonte longo, tolera volatilidade |
| Jéssica | 40% | 50% | 10% | Moderado — prioriza segurança na formação de patrimônio |
| Deusa | 40% | 25% | 35% | Conservador — preservação de capital e geração de renda |

Banda de tolerância: ±5 pontos percentuais. Desvio dentro da banda não gera
recomendação. Fora da banda, o rebalanceamento é feito **por aporte** (direcionar
dinheiro novo à camada defasada), nunca por venda, exceto quando o desvio
ultrapassa 15 pontos percentuais.

### Metas e limites

| Parâmetro | Valor | Aplicação |
|---|---|---|
| Reserva de emergência | R$100.000 em conjunto ou despesa mediana dos últimos 6 meses | Lucas e Jéssica (separados) |
| Reserva de emergência | R$100.000 | Deusa (aposentada) |
| Limite FGC por conglomerado | R$ 250.000 | Todos |
| Folga mínima sobre o limite FGC | R$ 50.000 | Alerta antes de estourar a garantia |
| Exposição internacional alvo | 15% da carteira | Lucas e Jéssica |
| Exposição internacional alvo | 10% da carteira | Deusa |
| Taxa de poupança alvo | 35% da receita líquida | Lucas e Jéssica (conjunta) |

### Benchmark

O sucesso do mês é medido contra o CDI e contra a inflação pessoal
(`minha_inflacao`), disponíveis no mart `riqueza`. Bater o CDI é a meta da
carteira; bater a inflação pessoal é a meta do patrimônio. As duas são
reportadas separadamente.

{% enddocs %}
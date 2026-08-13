{#
  ============================================================================
  Dicionário semântico do domínio FINANÇAS.

  Este arquivo é a FONTE ÚNICA de verdade sobre:
    1. o que cada categoria de gasto engloba;
    2. o que significa cada camada de investimento;
    3. a política de investimento (aporte, alocação-alvo, metas e limites);
    4. demais conhecimentos que auxiliem calcular e interpretar os dados

  FATOS RELEVANTES — cada categoria de gasto tem, no fim do seu bloco, uma
  subseção `Fatos relevantes`: eventos datados, fora da rotina, que explicam
  variação brusca e não existem em nenhum campo do dado. Uma linha por fato:

      - <período> — <justificativa breve>, <ordem de grandeza> (<status>).

  Status é `previsto` (pode não acontecer), ou `realizado` (ocorreu; anote o valor efetivo). Previsão e fato
  consumado convivem na mesma lista — quando a previsão se realiza muda só o
  status, não o lugar. Fato nenhum altera número: altera a leitura do número.
  Revise a cada fechamento e apague o que já não explica nada.

  É lido em três lugares:
    - pelo `dbt docs generate` (os blocos `{% docs %}` abaixo são referenciados
      em `_schema.yml` via `{{ doc('...') }}`);
    - pela skill `/relatorio-financas`, que usa estas definições para escrever
      o diagnóstico e as recomendações dos relatórios em PDF;
    - pelos dicionários `TEXTO_CATEGORIA`, `TEXTO_CAMADA`, `alvos_camada()`,
      `APORTE_ALVO` e `META_RESERVA_MESES` de
      `.claude/skills/relatorio-financas/scripts/montar_relatorio.py`, que são
      cópia resumida deste arquivo e vão impressos no PDF.

  Ao mudar uma regra de classificação de gasto ou de camada, edite AQUI —
  não no schema.yml e não na skill. Depois propague para os dicionários do
  `montar_relatorio.py`: eles não leem este arquivo, e já divergiram dele.

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

Fatos relevantes: nenhum registrado.

{% enddocs %}


{% docs categoria_diversos %}

**Diversos** — categoria residual.

Entra: vestuário e calçado, presentes, eletrônicos e acessórios pessoais, objetos
domésticos, imprevistos, clube de tiro, charutos, salão de beleza, e qualquer lançamento que não
caiba nas demais categorias.

Não entra: nada que tenha categoria própria. Se um tipo de gasto aparece em
`diversos` de forma recorrente e que não pode ser eliminado então ele deixou de ser residual e merece categoria
nova.

Natureza: variável, majoritariamente discricionário. Flags `fl_data_especial`e `fl_mes_especial` podem sinalizar
picos desse gasto.

Sinal de alerta: `diversos` acima de ~20% da despesa total do mês indica abuso de compras desnecessárias.
Relatório deve focar nessa categoria para redução de gastos.

Fatos relevantes:
- **06/08/2026 - 11/08/2026** — Viagem à Ouro Preto (Evento realizado)

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

Fatos relevantes: nenhum registrado.

{% enddocs %}


{% docs categoria_role %}

**Rolê** — lazer e consumo fora de casa.

Entra: bares e restaurantes, delivery e aplicativos de comida, cafés, cinema,
shows, eventos, viagens (hospedagem, aluguel de veículos, passeios), 
compras de mercado exclusivamente para eventos, hobbies e lazer em geral.

Não entra: transporte terrestre usado para chegar ao rolê (→ `transporte`), exceto aluguel de veículos;

Natureza: variável, discricionário. É o item que mais responde a decisão
consciente no curto prazo e o primeiro a ser revisto quando o resultado do mês
fica abaixo da meta de poupança. Flags `fl_data_especial`e `fl_mes_especial` podem sinalizar
picos desse gasto.

Fatos relevantes:
- **06/08/2026 - 11/08/2026** — Viagem à Ouro Preto (Evento realizado)

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

Fatos relevantes:

- **ago–set/2026** — troca de carro, ~R$ 15.000 (Evento previsto).

{% enddocs %}


{% docs categoria_apartamento %}

**Apartamento** — moradia e sua manutenção.

Entra: Condomínio, IPTU, energia elétrica, internet fixa, móveis, reformas e reparos.

Não entra: produtos de limpeza e itens de consumo da casa (→ `mercado`).

Natureza: fixa e essencial em quase toda a sua composição — é o piso do
orçamento. Móveis e reformas são exceções: são investimentos pontuais em bem
durável e devem ser lidos separadamente da despesa corrente de moradia.

Referência cruzada: a conta de energia tem detalhamento próprio no mart `luz`
(valor, kWh, consumo diário e preço por kWh), útil para separar aumento de
tarifa de aumento de consumo. `luz` **detalha uma parcela de `apartamento`** —
não é uma nona categoria e nunca deve ser somada a ela.

Fatos relevantes: nenhum registrado.

{% enddocs %}


{% docs categoria_saude %}

**Saúde** — saúde física e mental.

Entra: plano de saúde, consultas, exames, procedimentos, odontologia, terapia,
farmácia e medicamentos.

Não entra: academia (→ `assinaturas`).

Natureza: mista — plano de saúde é fixo e essencial; o restante é variável e
não compressível. Nunca deve ser objeto de recomendação de corte: quando
`saude` sobe, o relatório reporta e explica, não sugere reduzir.

Fatos relevantes:

- **set–dez/2026** — transplante capilar de Lucas, ~R$ 25.000 (evento previsto). Sai da
  `RESERVA` dele, ainda que a despesa seja lançada no casal.
- **até out/2026** — sobreposição do plano de saúde da empresa com o particular,
  ~R$ 650/mês adicional (evento previsto) descontado em folha de pagamento a partir de Set/2026. 
  Quando encerrar, a queda de `saude` não é mérito de contenção de gasto.

{% enddocs %}


{% docs categoria_educacao %}

**Educação** — formação e desenvolvimento.

Entra: cursos, graduação e pós-graduação, certificações, livros, material
didático e plataformas de ensino.

Não entra: assinatura de plataforma de conteúdo genérico (→ `assinaturas`);

Natureza: variável, discricionário no curto prazo mas com retorno esperado no
longo prazo. Deve ser tratado no relatório como investimento em capital humano,
não como consumo — não entra nas sugestões de corte por padrão.

Fatos relevantes:
- **Maio/2026** Festa do Livro UNESP 50% desconto (Evento realizado).

{% enddocs %}

{% docs ajuste %}

**Ajuste Realizado** — Equilíbrio de despesas conjuntas.

Lucas centraliza o pagamento das despesas compartilhadas; a Jéssica compensa
mensalmente para que cada um arque com uma parcela proporcional à sua renda. O
valor da compensação já desconta as despesas individuais do Lucas.

**Sinal:** positivo é o valor que a Jéssica transferiu ao Lucas no mês — crédito
para ele, débito para ela.

**Fora do DRE:** o ajuste **não** está dentro de `total_receita` nem de
`total_despesas` — é lançado depois do registro das movimentações e aparece ao
lado delas apenas como referência de análise. Somá-lo à receita conta o mesmo
dinheiro duas vezes. Os saldos de conta corrente, esses sim, já o refletem.

{% enddocs %}


{% docs ciclo_fatura %}

**Ciclo de fatura** — por que existem duas datas para o mesmo gasto.

Uma despesa no cartão é paga num mês e pertence a outro. O domínio guarda os
dois, e confundi-los troca o mês inteiro de lugar:

| Campo | O que é |
|---|---|
| `mes_debito` | **Mês de competência** — a que mês o gasto pertence. É a chave de todas as análises: DRE, gasto por categoria, resultado, taxa de poupança. |
| `mes_fatura` | Mês da fatura em que a despesa foi efetivamente cobrada. Serve para conciliar com o extrato do cartão, não para analisar comportamento. |
| `dia_real` | Dia do calendário em que o gasto ocorreu. |
| `dia_ajustado` | O mesmo dia deslocado para a posição que ocupa dentro do ciclo de fatura, para que gastos de ciclos diferentes sejam comparáveis dia a dia. |

O dia de fechamento do cartão que define o ciclo é aplicado **na planilha de
origem**: `dia_ajustado` e `dia_real` chegam prontos em `raw.luc_contas` e
`raw.jsc_contas`, e nenhum modelo dbt os recalcula. Para mudar a regra do ciclo,
edite a planilha — não os modelos.

{% enddocs %}


{% docs camada_investimento %}


**Camada de alocação** — o papel que o ativo cumpre na carteira conforme intenção do investidor.

A camada não descreve o que o ativo é (isso é `tipo_ativo`), e sim para
que ele existe na carteira. É classificada manualmente por Lucas na aba
`classificacao` da planilha e volta ao warehouse via
`stg_carteira_classificacao`, com histórico SCD2 (join as-of por `mes_base`).

| Camada | Objetivo | Horizonte | Instrumentos típicos |
|---|---|---|---|
| `RESERVA` | Liquidez e preservação de capital | D+0 a D+1 | CDB e RDB de liquidez diária, conta remunerada, saldo em conta corrente |
| `RESERVA ESTRATEGICA` | Reserva de valor | Indefinido | Criptoativos, saldo em moeda estrangeira, cashback |
| `CRESCIMENTO` | Acumulação de patrimônio, aceita volatilidade | 5 anos ou mais | RDB de vencimento, CDB, LCA, LCI, ações, ETFs, BDRs, fundos de ações, cripto, prefixado e IPCA+ longos |
| `RENDA` | Geração de fluxo de caixa recorrente | Indefinido — a posição é para carregar | FIIs, ações pagadoras de dividendos |
| `NAO CLASSIFICADO` | — | — | Default automático a ser regularizado |

**Toda camada é atribuída manualmente**, na aba `classificacao` da planilha. O
default automático de qualquer posição nova — inclusive das disponibilidades em
conta — é `NAO CLASSIFICADO`. Nenhuma camada é inferida pelo modelo.

`RESERVA` — um ativo é reserva quando cumpre os **três** critérios, nesta ordem:

1. resgate em até D+1;
2. sem marcação a mercado negativa no resgate;
3. a intenção de uso é cobrir despesa, não carregar o papel até o vencimento.

O critério (3) é o que decide os casos ambíguos, e é por ele que um CDB com
vencimento em 3 anos **não** é reserva mesmo tendo liquidez em D+1: a intenção
é levá-lo ao vencimento, então ele é `CRESCIMENTO`. Liquidez sozinha não basta.

O tamanho da reserva é definido em meses de despesa com piso em reais (ver
"Metas e limites"), não em percentual da carteira: reserva existe para cobrir
despesas correntes e emergenciais, não para acompanhar o patrimônio uma vez que
as metas estejam cumpridas.

A reserva pode estar comprometida com saída já planejada — hoje, o transplante
capilar de Lucas (ver "Fatos relevantes" em `categoria_saude`). Quando houver um
fato desses, a calibragem da carteira usa a reserva **menos** o compromisso, não
o saldo bruto, e a queda no mês da saída é execução do plano, não
desenquadramento da alocação-alvo.

`RESERVA ESTRATEGICA` — ativos de reserva de valor (cripto, moeda estrangeira,
cashback). Têm alta liquidez, mas dependem de um cenário favorável de valorização
para serem liquidados, e não há perspectiva de novos aportes. Por isso **fica
fora da alocação-alvo**: não faz sentido ter meta de rebalanceamento para uma
posição que não recebe aporte. No relatório ela aparece com o valor e o
percentual que representa, e com alvo `—`.

`CRESCIMENTO` — é a camada que aceita perda temporária em troca de retorno
esperado maior. Renda fixa longa entra aqui, e não em `RESERVA`, quando está
sujeita a marcação a mercado. Investimentos no exterior que rendem dividendos
também são considerados aqui.

`RENDA` — o que qualifica é a previsibilidade do fluxo, não o rendimento total
nem o prazo. Sobreposição com `CRESCIMENTO` é esperada (uma ação pagadora de
dividendo também se valoriza); a classificação segue a intenção de uso do ativo.

`NAO CLASSIFICADO` — não é uma camada, é ausência de classificação. Um ativo cai
aqui quando é novo na carteira ou quando sua chave de identidade mudou (`pessoa`,
`ativo`, `classe_ativo`, `tipo_ativo`, `instituicao`).
Tickers de renda variável são estáveis; descrições de renda fixa podem mudar e
quebrar o vínculo. Todo `NAO CLASSIFICADO` no mês corrente é pendência
operacional e deve aparecer no relatório como item de ação, não como alocação.

{% enddocs %}


{% docs politica_investimentos %}

**Política de investimento** — parâmetros que o relatório usa para transformar
diagnóstico em recomendação.

Sem estes números, o relatório só consegue descrever a carteira. Com eles,
consegue dizer para onde vai o aporte do mês.

### Aporte mensal estimado

| Pessoa | Aporte alvo estimado (R$/mês) | Origem |
|---|---|---|
| Lucas | 4.000  | Resultado mensal (receita − despesa) |
| Jéssica | 2.000  | Resultado mensal (receita − despesa) |
| Deusa | 3.000 | Renda própria |

Regra de aporte: o valor efetivo do mês é o resultado apurado no mart
`resultado`, não o alvo. O alvo serve para medir aderência — quando o resultado
fica abaixo dele, o relatório aponta a categoria de despesa que explica a
diferença.

### Alocação-alvo por camada

| Pessoa | RESERVA | CRESCIMENTO | RENDA | Razão da alocação |
|---|---|---|---|---|
| Lucas | 30% | 50% | 20% | Horizonte longo com tolerância a volatilidade; a fatia em `RENDA` existe para começar a formar fluxo de caixa antes de precisar dele |
| Jéssica | 30% | 70% | 0% | Fase de acumulação pura — sem necessidade de fluxo corrente, todo o risco vai para crescimento |
| Deusa | 30% | 60% | 10% | Aposentada, mas com reserva já formada e renda própria cobrindo a despesa; o crescimento serve à sucessão, não ao consumo |

`RESERVA ESTRATEGICA` **não tem alvo** — ver `camada_investimento`. `NAO
CLASSIFICADO` também não: é pendência, não alocação.

**Denominador:** os percentuais acima são sobre a carteira **excluindo**
`RESERVA ESTRATEGICA` e `NAO CLASSIFICADO`. As três camadas com alvo somam 100%
dessa base. O valor das duas camadas sem alvo é reportado à parte, em reais e
como percentual da carteira total.

Banda de tolerância: ±5 pontos percentuais. Desvio dentro da banda não gera
recomendação. Fora da banda, o rebalanceamento é feito **por aporte** (direcionar
dinheiro novo à camada defasada), nunca por venda, exceto quando o desvio
ultrapassa 15 pontos percentuais **medidos contra o alvo** (não contra a borda
da banda) — ou seja, uma camada com alvo 50% só autoriza venda abaixo de 35% ou
acima de 65%.

### Metas e limites

**Reserva-alvo** — uma regra só, em vez de duas unidades concorrentes:

> Reserva-alvo = **o maior** entre
> (a) N × mediana da despesa total dos últimos 6 meses fechados, e
> (b) R$ 100.000.

| Pessoa | N | Base de despesa |
|---|---|---|
| Lucas e Jéssica | 6 meses | Despesa do casal, avaliada em conjunto |
| Deusa | 12 meses | Aposentada; não há lançamento de despesa dela no warehouse, então na prática vale o piso de R$ 100.000 |

A mediana (e não a média) é o que dimensiona a reserva, para que um mês atípico
— IPVA, viagem, reforma — não infle a meta permanentemente. Meses futuros
pré-lançados em `resultado` não entram na base.

**Demais parâmetros**

| Parâmetro | Valor | Aplicação |
|---|---|---|
| Limite FGC por conglomerado | R$ 250.000 | Todos |
| Folga mínima sobre o limite FGC | R$ 50.000 | Alerta antes de estourar a garantia; é o limiar usado nos marts `risco_fgc_*` |
| Exposição internacional alvo | 15% da carteira  | Lucas e Jéssica |
| Exposição internacional alvo | 10% da carteira  | Deusa |
| Taxa de poupança alvo | 35% da receita líquida | Lucas e Jéssica (conjunta) |

### Benchmark

O sucesso do mês é medido contra o CDI e contra a inflação pessoal
(`minha_inflacao`), disponíveis no mart `riqueza`. Bater o CDI é a meta da
carteira; bater a inflação pessoal é a meta do patrimônio. As duas são
reportadas separadamente.

{% enddocs %}
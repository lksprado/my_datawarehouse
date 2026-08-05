{#-
  De-para único das instituições do domínio finanças.

  As variantes vêm com a grafia de cada fonte (B3, Avenue, seeds de
  investimentos faltantes) e são reduzidas ao nome padronizado que a carteira
  usa como coluna em int_carteira_agregada. Adicionar uma variante nova é uma
  linha AQUI — antes o mesmo CASE estava copiado em int_renda_variavel,
  int_renda_fixa e int_renda_passiva, e as três cópias já divergiram: o CDB
  Daycoval do seed de faltantes (que chega como 'DAYCOVAL', não
  'BANCO DAYCOVAL S/A') caía em 'DESCONHECIDO'.

  O fallback é 'DESCONHECIDO' de propósito: saldo diferente de zero nessa
  instituição é sinal de variante não mapeada, não de uma instituição real.
  Vale conferir depois de cada carga nova.

  `indent` é o recuo (em espaços) da coluna no SELECT que chama a macro, só
  para o SQL compilado sair legível e passar no sqlfluff.
-#}
{% macro normaliza_instituicao(field='instituicao', indent=8) -%}
{%- set espaco = ' ' * indent -%}
{%- set de_para = {
    'DAYCOVAL': ['BANCO DAYCOVAL S/A', 'DAYCOVAL'],
    'NUBANK': ['NU INVESTIMENTOS S.A. - CTVM', 'EASYNVEST - TITULO CV S/A', 'NU INVEST CORRETORA DE VALORES S.A.', 'NUBANK'],
    'BRADESCO': ['BANCO BRADESCO S/A'],
    'ITAU': ['ITAU UNIBANCO S.A.', 'ITAU'],
    'SOFISA': ['BANCO SOFISA S/A', 'SOFISA'],
    'AVENUE': ['AVENUE'],
    'BANCO DO BRASIL': ['BANCO DO BRASIL S/A', 'BB BANCO DE INVESTIMENTO S/A']
} -%}
CASE
{%- for padronizado, variantes in de_para.items() %}
{{ espaco }}    WHEN {{ field }} IN ({{ "'" ~ (variantes | join("', '")) ~ "'" }}) THEN '{{ padronizado }}'
{%- endfor %}
{{ espaco }}    ELSE 'DESCONHECIDO'
{{ espaco }}END
{%- endmacro %}

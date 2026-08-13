# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **dbt (data build tool) project** for a personal multi-domain data warehouse running on **PostgreSQL**. It is used as a **Git submodule** inside an Airflow environment orchestrated via Astro Cosmos. All SQL is PostgreSQL — not Snowflake or BigQuery.

## Common Commands

```bash
# Validate connection
dbt debug

# Run all models
dbt run

# Run a single model
dbt run --select int_carteira

# Run a domain using selectors (energia | livros | inflation)
dbt run --selector energia

# Run a domain by tag (financas | datas)
dbt run --select tag:financas

# Run a model and all its downstream dependents
dbt run --select int_carteira+

# Run tests
dbt test
dbt test --select int_carteira

# Load the seeds (several finanças models depend on them)
dbt seed

# Generate and browse docs
dbt docs generate && dbt docs serve

# Install packages
dbt deps
```

## Architecture

### Layer conventions

| Layer | Materialization | Schema | Prefix | Purpose |
|-------|----------------|--------|--------|---------|
| Staging | `table` | `staging` | `stg_` | Extract and type-cast raw sources (JSON payloads, Google Sheets exports, seeds); add indexes via `post_hook` |
| Intermediate | `view` | `intermediate` | `int_` (`int_dim_` / `int_fct_` in NHL) | Business logic joins; Kimball dimensions and facts |
| Marts | `table` | `marts` | none in most domains, `mrt_` in energy | Analytics-ready models |

> **Materialization:** every mart is a `table` (`dbt_project.yml` → `marts: +materialized: table`). Some models set `materialized = 'table'` explicitly in their config, which is redundant but harmless. Nothing in the project uses PostgreSQL `MATERIALIZED VIEW` except the NHL parameter views — so no `REFRESH MATERIALIZED VIEW` step is needed anywhere.

### Domain structure

Each domain lives under `models/staging/<domain>/`, and — where it has downstream logic — `models/intermediate/<domain>/` and `models/marts/<domain>/`.

**Finanças** is the active domain, and the only one with a semantic dictionary. It is spread across several staging folders that all feed `models/{intermediate,marts}/financas/`:

- **google** — Google Sheets export: contas, consolidado, patrimônio, luz, ajuste, classificação de carteira
- **b3** — B3 positions: ações, BDR, ETF, fundos, renda fixa, tesouro direto, proventos
- **avenue** — Avenue (broker no exterior): assets e dividendos
- **seeds_sources** — seeds: câmbio USD, de-para FGC, investimentos faltantes, datas especiais

Other domains:

- **inflation** — Price tracking from Atacadão and Minha Inflação scrapers
- **livros** — Bookstore price history from Vide Editorial scraping
- **solar** + **weather** — Residential IoT solar generation + OpenWeather API (mart `energy`, selector `energia`)
- **conformado** — `int_dates` / `dim_datas`, the conformed date dimension shared by all domains
- **nhl** — NHL hockey analytics. **Currently disabled**: `dbt_project.yml` sets `+enabled: false` for both `staging.nhl` and `intermediate.nhl`, so these models do not build and are excluded from `dbt run`. The code is kept in the repo.

### Finanças — read this before touching the domain

`models/marts/financas/_docs_financas.md` is the single source of truth for spending categories, investment layers and the investment policy (target allocation, contribution targets, reserve, FGC limits). Change a rule **there**, not in a `schema.yml`.

Its numeric parameters are duplicated in `scripts/relatorios/politica.py` (`ALVOS_CAMADA`, `APORTE_ALVO`, `META_RESERVA_*`, `META_POUPANCA_PCT`, `TEXTO_CATEGORIA`, `TEXTO_CAMADA`) because the report builders cannot read Markdown. **Edit both in the same pass** — they silently diverged once and the monthly PDF rendered targets that contradicted the written policy. There is exactly **one** Python copy, shared by both report skills; do not make a third.

### Two reports, two cadences

The sources do not become trustworthy at the same time — `{% docs calendario_dados %}` in `_docs_financas.md` is the table. Spend is daily; the DRE closes in the first days of the next month; the carteira closes on its own cadence; **the indexadores (IPCA/CDI/Selic/inflação pessoal) only land around day 10**, because the IPCA is published then and the spreadsheet is filled afterwards. Hence two skills, both **on-demand only** — nothing schedules them, no DAG, no cron:

| | `relatorio-financas` | `relatorio-meio-mes` |
|---|---|---|
| Runs | days 1–5 | days 15–20 |
| Reference | the **previous**, closed month | the **current** month (+ benchmark of the previous) |
| Output | 4 PDFs — one per titular + the couple's budget | 1 PDF, the couple's |
| Parameter | a month (`AAAA-MM`) | a **date** (`AAAA-MM-DD`), so a past day can be reproduced |
| Gates in `meta.prontidao` | `pronto_orcamento` (needs the DRE), `pronto_investimentos` (needs the carteira in date) | `pronto_ritmo` (day ≥ 10, entries fresh), `pronto_indicadores` (previous month's IPCA filled) |
| Batch path | `scripts/gerar_relatorios_financas.sh` | `scripts/gerar_relatorio_meio_mes.sh` |

`relatorio-financas` resolves its month from `date` (never `MAX(mes)`, never the current month). Investment is individual, receita/despesa is joint, and the emergency reserve lives in the budget report because it is sized by the couple's median expense.

`relatorio-meio-mes` answers "what to do in the days that remain": accumulated spend against the min–max envelope and median of the six closed months **on the same day of the billing cycle** (`dia_fatura`, never `dia_mes`), a projected close, and the margin still available in the compressible categories. **The benchmark section lives here, not in the closing report** — `riqueza` INNER JOINs the indexadores, so on day 1 the reference month is simply absent and the series used to shrink by one month in silence. The mart `indicadores` exists to make that legible: unlike `riqueza` it keeps the row with `NULL`, which is what `pronto_indicadores` reads.

Projection rule (documented in `{% docs cadencia_relatorios %}`): `realizado + GREATEST(median of what fell after the cut in the 6 closed months, what is already booked with a future date)`. `GREATEST` and not a sum, because the historical median already embeds the day-25 fixed expenses. It is biased high by construction, and **the per-category column does not add up to the total** — the median of a sum is not the sum of the medians. Both facts are printed in the PDF; never reconcile them in the narrative.

### Shared rendering layer

`scripts/relatorios/` is a Python package imported by **both** skills: `politica` (the policy parameters above), `formato` (pt-BR money/percent/date), `calculo` (`media`/`mediana`/`variacao`), `graficos` (hand-written inline SVG — no chart library), `blocos` (KPI/table/section/glossary HTML) and `relatorio.css`. Capa, rodapé, readiness banner and section order stay in each skill's montador, because the procedência text and the gates differ. Touching `scripts/relatorios/` changes both PDFs — check both. The montadores bootstrap it with `sys.path.insert(0, parents[4] / "scripts")`.

### Key patterns

**JSON denormalization at staging:** Scraper and API sources store a single `payload jsonb` column. Staging models cast every field explicitly, e.g. `(payload ->> 'id')::int as game_id`. Never reference raw `payload` columns downstream of staging. Google Sheets and seed sources are not JSON — they arrive as text columns and are cleaned with the `clean_string` / `clean_integer` macros.

**De-para de instituições:** `macros/normaliza_instituicao.sql` é a fonte única do mapeamento variante → nome padronizado da instituição, chamada por `int_renda_variavel`, `int_renda_fixa` e `int_renda_passiva`. Grafia nova de uma fonte se resolve com uma linha no dicionário `de_para` da macro — antes o mesmo `CASE` estava copiado nos três modelos e as cópias divergiram. O fallback `'DESCONHECIDO'` é o detector: saldo diferente de zero nessa instituição em `int_carteira_agregada` significa variante não mapeada.

**Ephemeral base models:** Some staging models have a helper that does the parsing shared by two sibling staging tables — `stg_base_*` in the NHL domain, `bases/` subfolder in the inflation domain.

**SCD2 by as-of join:** The investment layer (`camada`) is classified by hand in a spreadsheet and read back through `stg_carteira_classificacao`. `int_carteira` and `int_carteira_extra` resolve it with a `LEFT JOIN LATERAL` picking the last classification with `mes_base <= ` the position's month, so past months keep the classification that was in force then. Unclassified positions fall back to `'NAO CLASSIFICADO'`.

**Parameter views:** `models/staging/nhl/parameters/vw_stg_request_*.sql` are not standard staging models — they are `materialized_view` query helpers that the Airflow extraction layer reads to determine which records to fetch next. They follow a different convention (`vw_` prefix) intentionally.

**Incremental loading:** Incremental models (`stg_all_play_by_play`, `stg_all_games_details`) use `delete+insert` strategy with composite unique keys. Always use PostgreSQL date arithmetic (`- interval '3 days'`), not `dateadd()` (Snowflake syntax).

**Index post-hooks:** Staging tables add indexes in `post_hook` using `CREATE INDEX IF NOT EXISTS`. Composite indexes exist on high-cardinality join keys (`game_id`, `event_id`, `game_date`).

**Domain selectors:** `selectors.yml` defines path-based selectors for `energia`, `livros` and `inflation`. Finanças and conformado have **no selector** — select them by tag (`--select tag:financas`, `--select tag:datas`).

### Schema/YAML files

- `models/staging/_sources.yml` — all 36 raw source tables
- `models/staging/<domain>/_schema.yml` — staging documentation and tests, one file per domain folder
- `models/intermediate/<domain>/_schema.yml` — intermediate documentation and tests
- `models/marts/<domain>/_schema.yml` — mart documentation
- `models/marts/financas/_docs_financas.md` — `{% docs %}` blocks shared by the finanças schemas

**Naming:** use `_schema.yml` (leading underscore). Two folders still use `schema.yml` — `intermediate/financas`, `intermediate/livros`, `intermediate/nhl`, `marts/energy`, `marts/inflation`, `marts/livros`. Rename on next touch; dbt does not care about the filename.

## Dependencies

- `dbt-core ^1.10.0`, `dbt-postgres ^1.10.0`
- `dbt-labs/dbt_utils 1.3.3` — `unique_combination_of_columns` generic test, `pivot` and `get_column_values`
- `calogica/dbt_date` — `get_date_dimension` macro behind `int_dates`
- `sqlfluff ^3.5.0` — SQL linting with dbt templating support

## Known Gaps (fora do escopo atual)

- `dbt_utils.get_column_values(..., default=[...])` devolve `Undefined` — e não o `default` — na fase de parse (dbt_utils 1.4.1 + Jinja 3.1). O `default` só vale em runtime, quando a relação não existe. Operar sobre o retorno (`+`, `| unique`) sem um guard em `execute` quebra o parse; ver o header de `int_carteira_agregada.sql`.
- `int_carteira_extra.sql`: o fallback de camada é `'NAO CLASSIFICADO'`, mas a intenção registrada era `'RESERVA ESTRATEGICA'` — saldo em conta, que tem camada natural, aparece como pendência de classificação na planilha.
- Reserva-alvo: o N em meses de despesa (6 casal / 12 Deusa) está marcado `[CONFIRMAR]` na política — nunca foi validado.
- Sources sem `freshness:` — nenhuma source em `_sources.yml` tem alerta de dados desatualizados configurado.
- Domínio NHL desabilitado (`+enabled: false`): `stg_all_players.sql` duplica ~45 linhas de extração JSON entre `regular` e `playoffs`, e `stg_all_games_details` usa incremental por `game_id > max(game_id)`, que não cobre backfills.

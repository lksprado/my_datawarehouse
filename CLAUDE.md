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
dbt run --select stg_all_games_summary

# Run a domain using selectors (energia | nhl | livros | inflation)
dbt run --selector nhl

# Run a model and all its downstream dependents
dbt run --select stg_all_games_summary+

# Run tests
dbt test
dbt test --select int_dim_games

# Full refresh of an incremental model
dbt run --select stg_all_play_by_play --full-refresh

# Generate and browse docs
dbt docs generate && dbt docs serve

# Install packages
dbt deps
```

## Architecture

### Layer conventions

| Layer | Materialization | Schema | Prefix | Purpose |
|-------|----------------|--------|--------|---------|
| Staging | `table` | `staging` | `stg_` | Extract and type-cast JSON payloads from raw sources; add indexes via `post_hook` |
| Intermediate | `view` | `intermediate` | `int_dim_` / `int_fct_` | Kimball-style dimensions and facts; business logic joins |
| Marts | `materialized_view` | `marts` | `mrt_` | Analytics-ready joins across intermediate models |

> **Important:** PostgreSQL `MATERIALIZED VIEW` is not auto-refreshed by `dbt run` — it requires a `REFRESH MATERIALIZED VIEW` post-hook or manual execution to update data after the initial build.

### Domain structure

Each domain lives under `models/staging/<domain>/`, `models/intermediate/<domain>/`, and `models/marts/<domain>/`. Four domains exist:

- **nhl** — NHL hockey analytics (game summaries, play-by-play, player stats, fights)
- **inflation** — Price tracking from Atacadão and Minha Inflação scrapers
- **livros** — Bookstore price history from Vide Editorial scraping
- **solar_weather_project** — Residential IoT solar generation + OpenWeather API

### Key patterns

**JSON denormalization at staging:** All raw tables store a single `payload jsonb` column. Staging models cast every field explicitly, e.g. `(payload ->> 'id')::int as game_id`. Never reference raw `payload` columns downstream of staging.

**Ephemeral base models:** Some staging models have an ephemeral helper (prefixed `stg_base_` in the NHL domain, placed in `bases/` in the inflation domain) that does the common parsing shared by two sibling staging tables. The NHL domain uses `stg_base_*` naming; the inflation domain uses `eph_*` naming — this inconsistency is a known issue.

**Parameter views:** `models/staging/nhl/parameters/vw_stg_request_*.sql` are not standard staging models — they are `materialized_view` query helpers that the Airflow extraction layer reads to determine which records to fetch next. They follow a different convention (`vw_` prefix) intentionally.

**Incremental loading:** Incremental models (`stg_all_play_by_play`, `stg_all_games_details`) use `delete+insert` strategy with composite unique keys. Always use PostgreSQL date arithmetic (`- interval '3 days'`), not `dateadd()` (Snowflake syntax).

**Index post-hooks:** Staging tables add indexes in `post_hook` using `CREATE INDEX IF NOT EXISTS`. Composite indexes exist on high-cardinality join keys (`game_id`, `event_id`, `game_date`).

**Domain selectors:** `selectors.yml` defines path-based selectors for each domain. Use `dbt run --selector nhl` to run only NHL staging + all downstream models.

### Schema/YAML files

- `models/staging/_sources.yml` — all 17 raw source tables
- `models/staging/_schema.yml` — all staging model documentation and tests
- `models/intermediate/<domain>/schema.yml` — intermediate model tests
- `models/marts/<domain>/*.yml` — mart documentation (partial)

## Dependencies

- `dbt-core ^1.10.0`, `dbt-postgres ^1.10.0`
- `dbt-labs/dbt_utils 1.3.3` — used for `unique_combination_of_columns` generic test
- `sqlfluff ^3.5.0` — SQL linting with dbt templating support

## Known Gaps (fora do escopo atual)

- `stg_all_players.sql` duplica ~45 linhas de extração JSON para `regular` e `playoffs` — diferem apenas em `careerTotals -> 'regularSeason'` vs `'playoffs'`
- Sources sem `freshness:` — nenhuma source em `_sources.yml` tem alerta de dados desatualizados configurado
- `stg_all_games_details` usa incremental por `game_id > max(game_id)` — não cobre backfills; considerar migrar para filtro por data
- Marts usam `materialized_view` do PostgreSQL (não auto-refreshadas) — requerem `REFRESH MATERIALIZED VIEW` externo ao dbt

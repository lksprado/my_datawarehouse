# Copilot Instructions for my_datawarehouse

## Project Overview

**my_datawarehouse** is a dbt project that builds a PostgreSQL data warehouse with three main data domains:
1. **NHL Hockey Data** - Game statistics, player performance, fights (from nhl-extraction)
2. **Solar Energy Data** - Residential solar generation (from Solar project)
3. **Weather Data** - Climate and weather measurements (from openweather)

The warehouse uses a **three-layer architecture**: staging → intermediate → marts.

## Architecture & Data Flow

### Layer Structure

- **Staging (raw_* prefix)**: Raw source tables from external systems in PostgreSQL `raw` schema
- **Staging Models (stg_* prefix)**: Cleaning & normalization layer; materialized as **tables** with indexes
  - NHL data: `stg_all_games_summary`, `stg_all_players`, `stg_all_play_by_play`, etc.
  - Solar/Weather: `stg_solar_daily_energy`, `stg_weather_daily`, etc.
- **Intermediate (int_* prefix)**: Business logic & transformations; materialized as **views**
  - Dimensions: `int_dim_games`, `int_dim_teams`, `int_dim_players`
  - Facts: `int_fct_games_events`, `int_fct_games_fights`, `int_fct_games_team_stats_long`
- **Marts (mrt_* prefix)**: Analytics-ready tables; materialized as **materialized views**
  - Example: `mrt_energia_clima` (solar energy joined with weather data)

### Key Data Relationships

NHL tables use **JSON payloads** at staging level (extract with `->>` operator), denormalizing in staging models:
```sql
(payload ->> 'id')::int as game_id
(payload ->> 'homeTeamId')::int as home_team_id
```

Critical dimensions:
- **season_id**: Hockey season identifier (e.g., 19851986 for 1985-86)
- **game_type_id**: 1=preseason, 2=regular, 3=playoffs
- **game_id**: Unique game identifier (used as unique_key in staging)

### Schema Configuration

All schemas are managed via `dbt_project.yml` config (do NOT hardcode schema names):
- Staging models → `staging` schema
- Intermediate models → `intermediate` schema  
- Marts models → `marts` schema

Custom macro `generate_schema_name.sql` handles schema routing.

## Development Conventions

### Model Configuration

Every model must have config block with **tags**:
```jinja-sql
{{
  config(
    materialized = 'table',  -- or 'view' for intermediate, 'materialized_view' for marts
    tags = ['nhl', 'staging'],  -- Required: domain tag (nhl/solar/weather) + layer tag
    unique_key = 'game_id',  -- Use for staging incremental models
  )
}}
```

### Common Patterns

**1. Multi-step CTEs** - Always use this sequence:
- `source/renamed`: Extract & clean raw data
- Intermediate CTEs: Business logic steps (each CTE = one responsibility)
- `final`: Select from last CTE before returning

**2. Incremental Staging Models** (NHL data):
```jinja-sql
{% if is_incremental() %}
where game_date >= dateadd(day, -3, current_date)  -- 3-day lookback for updates
{% endif %}
```

**3. Joins in Intermediate Models** - Use semantic naming:
```jinja-sql
left join teams as t2
  on t1.team_1_code = t2.team_code 
  and t1.season_id >= t2.first_season_id  -- Always season-date qualify NHL joins
```

**4. Testing** - Use dbt_utils for combination uniqueness on facts:
```yaml
tests:
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns:
        - game_id
        - event_id
```

### File Naming Conventions

- Staging: `stg_{source}_{entity}.sql` (e.g., `stg_all_games_summary.sql`)
- Intermediate: `int_{type}_{entity}.sql` where type is `dim_` (dimension) or `fct_` (fact)
- Marts: `mrt_{use_case}.sql` (e.g., `mrt_energia_clima.sql`)

## Common Tasks

### Adding a New Staging Model

1. Define source in `models/staging/_sources.yml` under `sources.raw`
2. Create `stg_xxx.sql` with `materialized='table'` config
3. Extract JSON fields using `payload ->>` operators
4. Add type casting (`::`int, `::date`, etc.)
5. Apply season_id filtering for NHL data (e.g., `where season_id >= 19851986`)
6. Add to appropriate test file (`_schema.yml`)

### Adding Intermediate Dimensions or Facts

1. Create `int_dim_xxx.sql` or `int_fct_xxx.sql`
2. Set `materialized='view'` (views are rebuilds every run)
3. Source from staging models using `{{ ref('stg_xxx') }}`
4. Add uniqueness tests for dimensions, combination tests for facts
5. Add tags by domain: `['nhl', 'intermediate']`

### Running dbt Locally

```bash
dbt debug                    # Verify connection to raw schema
dbt run --tag nhl           # Run only NHL models
dbt run --models int_dim_*  # Run all intermediate dimensions
dbt test                    # Run all tests
dbt docs generate           # Generate documentation
```

## External Dependencies

- **dbt-postgres** ≥1.10.0: PostgreSQL adapter
- **dbt-utils** 1.3.3: Generic tests (unique_combination_of_columns, etc.)
- **sqlfluff** ≥3.5.0: SQL linting/formatting with dbt templating support

## Important Context

### Season ID Format

NHL season IDs are **8 digits**: first 4 = start year, last 4 = end year (e.g., 19851986 = 1985-86 season). Use this for all season filtering.

### Data Freshness Strategy

- **Raw tables**: Updated daily via external extraction jobs
- **Staging tables**: Incremental models with 3-day lookback (handles late updates)
- **Intermediate views**: Rebuild each dbt run (fast due to materialized staging)
- **Marts**: Materialized views for query performance on analytics

### Schema Qualification

Always use source/ref functions for cross-schema references:
- Never hardcode `staging.table_name` — use `{{ ref('stg_xxx') }}`
- Never hardcode `raw.nhl_raw_games` — use `{{ source('raw', 'nhl_raw_all_games_summary') }}`

## Future Roadmap (2026)

- Consumer price data for inflation monitoring
- Online bookstore price data for price monitoring

Coordinate new data domains with same architecture (staging → intermediate → marts) and naming conventions.

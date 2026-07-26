# My Datawarehouse

## Overview

This repository serves as a **Git submodule** within an Airflow environment, implementing enterprise-grade data modeling practices for multi-domain analytics. It consolidates data from IoT sensors, sports analytics APIs, and weather services into analytics-ready datasets using dbt (data build tool).

### Purpose & Architecture

- **Data Centralization**: Consolidates distinct analytical domains (Personal Finance, Residential Solar Energy IoT, Weather Data, Inflation tracking, E-commerce products of interest, NHL Sports Analytics);
- **Scalable Orchestration**: Integrated with Airflow DAGs for automated, scheduled transformations using Astro Cosmos;
- **Analytics-Ready**: Follows **dbt** three-layer medallion architecture (staging → intermediate → marts);
- **Production Patterns**: Implements incremental loading, Kimball modelling, testing and documentation best practices;


## The data projects I have been working on...

### 0. 💰 **Personal Finance** *(main domain)*
- **Sources**: Google Sheets exports (accounts, consolidated P&L, net worth, electricity bill,
  portfolio classification), B3 position files, Avenue (US broker), and seeds (USD rate, FGC
  institution mapping, special dates)
- **Content**: Monthly net worth, income statement by category, investment portfolio by layer and
  institution, passive income, FGC exposure, and wealth growth against CDI / IPCA / personal inflation
- **Frequency**: Monthly close
- **Key Models**:
  - **Facts**: `resultado` (P&L), `consumo` (daily spend), `patrimonio` (net worth), `dividendos`
  - **Portfolio**: `carteira_lucas_jessica`, `carteira_deusa`, `risco_fgc_*`
  - **Features**: SCD2 allocation layer via as-of join, feedback loop back into the spreadsheet
    (`carteira_classificacao`), and a semantic dictionary (`_docs_financas.md`) that drives both the
    dbt docs and the monthly PDF report

### 1. 🏒 **NHL Hockey Analytics** *(currently disabled in `dbt_project.yml`)*
- **Sources**: \
  [nhl-extraction](https://github.com/lksprado/nhl-extraction) — Custom API extraction layer \
  [hockey-fights](https://github.com/lksprado/hockey-fights) — Webscraping [hockeyfights.com](https://www.hockeyfights.com/) website for fighting information
- **Content**: Game summaries, play-by-play events, player statistics, fights, team performance metrics
- **Frequency**: Daily updated
- **Scope**: Historical NHL data since 1917-18 season to present day (2025-26)
- **Key Models**:
  - **Dimensions**: Games, Players, Teams
  - **Facts**: Game events, fights, team  and player stats (long & wide formats)
  - **Features**: JSON payload denormalization at staging level, season-qualified joins, indexing and incremental updates for performance

### 2. ☀️ **Residential Solar Energy & Weather**
- **Sources**: \
  [Solar Project](https://github.com/lksprado/Solar) — IoT sensor data collection \
  [OpenWeather API Integration](https://github.com/lksprado/openweather) — automated weather extraction
- **Content**: Daily solar generation and weather metrics (temperature, humidity, precipitation, wind patterns) from the region
- **Frequency**: Daily updated
- **Key Models**:
  - Daily and hourly energy for efficiency metrics
  - Daily weather aggregations
  - Joined both for understanding how weather conditions affects the solar energy generation.

### 3. 📚 **Bookstore E-commerce Tracking**
- **Sources**: \
  [Vide Editorial Scrap](https://github.com/lksprado/webscraping-bookstore) — Webscraping books from some categories of interest \
- **Content**: Books prices and discounts from November/2024 snapshot to present day (2026)
- **Frequency**: Daily sales updated and Weekly for product catalog 
- **Key Models**:
  - **Dimensions**: Books and Authors 
  - **Fact**: Book price history
- **Coming Soon**: Fetching book's metadata from Google, Goodreads...

## Data Architecture

### Medallion Architecture Pattern

| Layer | Type | Purpose | Example |
|-------|------|---------|---------|
| **Staging** | Tables + Indexes | Extract, clean, denormalize raw payloads | `stg_all_games_summary`, `stg_all_players` |
| **Intermediate** | Views | Dimension/fact tables, business logic joins | `int_dim_games`, `int_fct_games_events` |
| **Marts** | Tables | Dashboard-ready, cross-domain analytics | `mrt_energia_clima` (solar + weather), `carteira_lucas_jessica` (finance) |


## Technical Highlights

### Data Modeling Practices
**Incremental Loading**: 3-day lookback windows for late-arriving updates  
**Schema Management**: dbt configuration-driven schema routing (no hardcoded paths)  
**Semantic Joins**: Season-qualified NHL joins with dimension slowly changing tracking  
**Comprehensive Testing**: dbt tests for uniqueness, relationships, and data quality  
**JSON Denormalization**: Extract and type-cast complex payloads at staging layer  

### Key Technical Decisions
- **PostgreSQL**: Single source-of-truth database with raw/staging/intermediate/marts schemas
- **dbt**: Version-controlled, testable SQL transformations with documentation generation
- **Mart Tables**: marts are materialized as tables, rebuilt by `dbt run` — no manual refresh step
- **Airflow Integration**: External orchestration triggers dbt runs via CLI/API
- **Version Control**: Complete lineage and reproducibility through dbt manifest

### Configuration & Dependencies
```yaml
dbt-core: ^1.10.0
dbt-postgres: ^1.10.0
dbt-utils: 1.3.3 (for generic tests)
sqlfluff: ^3.5.0 (SQL linting with dbt templating)
```

## Project Structure

```
├── models/
│   ├── staging/               # Raw → Clean
│   │   ├── google/           # Spreadsheet exports (finance)
│   │   ├── b3/ avenue/       # Broker positions and dividends
│   │   ├── seeds_sources/    # Seeds: FX, FGC mapping, special dates
│   │   ├── solar/ weather/   # Solar + Weather models
│   │   ├── inflation/ livros/
│   │   └── nhl/              # NHL models (currently disabled)
│   ├── intermediate/          # Clean → Business Logic
│   │   ├── int_dim_*.sql     # Dimension tables
│   │   └── int_fct_*.sql     # Fact tables
│   └── marts/                 # Analytics-ready aggregations
├── macros/                    # dbt macros (schema generation, utilities)
├── tests/                     # dbt data quality tests
├── dbt_project.yml           # dbt configuration
└── profiles.yml              # Database connection (Airflow-managed)
```

---

## Running Locally / Integration with Airflow

### dbt CLI Commands
```bash
# Validate connection and configuration
dbt debug

# Run all models
dbt run

# Run a specific domain
dbt run --selector energia          # solar + weather
dbt run --select tag:financas       # personal finance

# Run specific layer (e.g., intermediate models)
dbt run --select int_dim_* int_fct_*

# Execute all tests
dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```

## Data Freshness Strategy

| Layer | Frequency | Pattern |
|-------|-----------|---------|
| **Raw Tables** | Daily (external jobs) | Full replacement from extraction services |
| **Staging Models** | Incremental | 3-day lookback; handles late arrivals |
| **Intermediate Views** | On-demand | Rebuilt each dbt run (cheap computation) |
| **Marts** | Materialized | Refreshed with staging updates (optimized for analytics) |


## 🎯 Roadmap

### Delivered since
- **Consumer Price Index Data**: inflation tracking (Atacadão + Minha Inflação)
- **Utility Consumption**: electricity bill, with price vs. consumption split (`luz`)
- **Personal Finance**: net worth, DRE, investment portfolio and monthly PDF reporting
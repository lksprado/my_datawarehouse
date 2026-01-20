# My Datawarehouse

**A scalable, modular data engineering project integrating personal and analytical data domains into a centralized PostgreSQL data warehouse, orchestrated via Apache Airflow.**

## Overview

This repository serves as a **Git submodule** within an Apache Airflow environment, implementing enterprise-grade data modeling practices for multi-domain analytics. It consolidates data from IoT sensors, sports analytics APIs, and weather services into analytics-ready datasets using dbt (data build tool).

### Purpose & Architecture

- **Data Centralization**: Consolidates three distinct analytical domains (NHL Sports Analytics, Residential Solar Energy, Weather Intelligence)
- **Scalable Orchestration**: Integrated with Airflow DAGs for automated, scheduled transformations
- **Analytics-Ready**: Three-layer medallion architecture (staging → intermediate → marts) designed for clean, versioned data pipelines
- **Production Patterns**: Implements incremental loading, testing, and documentation best practices

---

## Data Domains

### 1. 🏒 **NHL Hockey Analytics**
- **Source**: [nhl-extraction](https://github.com/lksprado/nhl-extraction) — custom Python extraction layer
- **Content**: Game summaries, play-by-play events, player statistics, fights, team performance metrics
- **Scope**: Historical NHL data across multiple seasons (since 1985-86)
- **Key Models**:
  - **Dimensions**: Games, Players, Teams (slowly changing)
  - **Facts**: Game events, fights, team statistics (long & wide formats)
  - **Features**: JSON payload denormalization at staging level, season-qualified joins, incremental updates

### 2. ☀️ **Residential Solar Energy**
- **Source**: [Solar Project](https://github.com/lksprado/Solar) — IoT sensor data collection
- **Content**: Daily and hourly solar generation, energy production efficiency, system performance
- **Frequency**: Real-time collection with daily aggregations
- **Key Models**:
  - Daily energy summaries with efficiency metrics
  - Hourly generation profiles for pattern analysis

### 3. 🌤️ **Weather Intelligence**
- **Source**: [OpenWeather API Integration](https://github.com/lksprado/openweather) — automated weather extraction
- **Content**: Daily weather metrics (temperature, humidity, precipitation, wind patterns)
- **Correlation Analysis**: Designed to intersect with solar generation for efficiency modeling
- **Key Models**:
  - Daily weather aggregations and extremes
  - Joined with solar data in analytics marts (`mrt_energia_clima`)

---

## Data Architecture

### Medallion Architecture Pattern

```
raw/ (PostgreSQL raw schema)
  └─→ staging/ (stg_* tables) — Cleaning, type casting, JSON denormalization
       └─→ intermediate/ (int_* views) — Business logic, dimensions, facts
            └─→ marts/ (mrt_* materialized views) — Analytics-ready aggregations
```

### Layer Responsibilities

| Layer | Type | Purpose | Example |
|-------|------|---------|---------|
| **Staging** | Tables + Indexes | Extract, clean, denormalize raw payloads | `stg_all_games_summary`, `stg_all_players` |
| **Intermediate** | Views | Dimension/fact tables, business logic joins | `int_dim_games`, `int_fct_games_events` |
| **Marts** | Materialized Views | Dashboard-ready, cross-domain analytics | `mrt_energia_clima` (solar + weather) |

---

## Technical Highlights

### Data Modeling Practices
✅ **Incremental Loading**: 3-day lookback windows for late-arriving updates  
✅ **Schema Management**: dbt configuration-driven schema routing (no hardcoded paths)  
✅ **Semantic Joins**: Season-qualified NHL joins with dimension slowly changing tracking  
✅ **Comprehensive Testing**: dbt tests for uniqueness, relationships, and data quality  
✅ **JSON Denormalization**: Extract and type-cast complex payloads at staging layer  

### Key Technical Decisions
- **PostgreSQL**: Single source-of-truth database with raw/staging/intermediate/marts schemas
- **dbt**: Version-controlled, testable SQL transformations with documentation generation
- **Materialized Views**: Performance optimization for analytics queries on high-cardinality facts
- **Airflow Integration**: External orchestration triggers dbt runs via CLI/API
- **Version Control**: Complete lineage and reproducibility through dbt manifest

### Configuration & Dependencies
```yaml
dbt-core: ^1.10.0
dbt-postgres: ^1.10.0
dbt-utils: 1.3.3 (for generic tests)
sqlfluff: ^3.5.0 (SQL linting with dbt templating)
```

---

## Project Structure

```
├── models/
│   ├── staging/               # Raw → Clean
│   │   ├── nhl/              # NHL models
│   │   └── solar_weather_project/  # Solar + Weather models
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

# Run specific domain (e.g., NHL data)
dbt run --tag nhl

# Run specific layer (e.g., intermediate models)
dbt run --models int_dim_* int_fct_*

# Execute all tests
dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```

### Airflow Integration
This repository is configured as a **Git submodule** within an Airflow instance:
- Airflow DAGs call `dbt run` commands via `dbtRunOperator` or `BashOperator`
- Source data freshness is managed by upstream extraction DAGs
- Transformations execute on a configurable schedule (daily incremental loads)
- Logs are centralized in Airflow UI for monitoring and troubleshooting

---

## Data Freshness Strategy

| Layer | Frequency | Pattern |
|-------|-----------|---------|
| **Raw Tables** | Daily (external jobs) | Full replacement from extraction services |
| **Staging Models** | Incremental | 3-day lookback; handles late arrivals |
| **Intermediate Views** | On-demand | Rebuilt each dbt run (cheap computation) |
| **Marts** | Materialized | Refreshed with staging updates (optimized for analytics) |

---

## 🎯 Roadmap 2026+

### Planned Data Domains
- **Consumer Price Index Data**: Inflation tracking and monitoring
- **Online Bookstore Price Data**: Price trend analysis and market monitoring
- **Utility Consumption**: Energy cost attribution and forecasting

All new domains will follow the same medallion architecture with consistent naming conventions and testing patterns.

---

## Key Takeaways for Engineers

🔹 **Multi-domain consolidation** with semantic layer separation  
🔹 **Production-grade patterns**: Incremental loading, testing, schema management  
🔹 **Scalable architecture**: Clean separation of concerns across three layers  
🔹 **Automation-ready**: Designed for orchestration within Airflow  
🔹 **Maintainability**: Comprehensive documentation, version control, and data lineage tracking  

---

## Resources

- **dbt Documentation**: [docs.getdbt.com](https://docs.getdbt.com)
- **Source Repositories**:
  - NHL Extraction: https://github.com/lksprado/nhl-extraction
  - Solar IoT: https://github.com/lksprado/Solar
  - Weather API: https://github.com/lksprado/openweather
- **Airflow Integration**: See parent repository for DAG configurations

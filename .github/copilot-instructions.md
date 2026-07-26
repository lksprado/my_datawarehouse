# Copilot Instructions for my_datawarehouse

The guidance for this repository lives in **[`CLAUDE.md`](../CLAUDE.md)** — read it first.

It is the single description of the project: layer conventions and materializations, the domain
map, the finanças semantic dictionary, key patterns (JSON denormalization, SCD2 by as-of join,
incremental loading, index post-hooks), selectors, dependencies and known gaps.

This file used to carry its own copy of that description. The copy drifted: it still claimed the
project had three domains, that marts were materialized views, and it taught
`dateadd(day, -3, current_date)` — Snowflake syntax that does not run on PostgreSQL and that
`CLAUDE.md` explicitly forbids. Rather than maintain two descriptions in parallel, this one was
replaced by a pointer.

**Do not restore a duplicated project description here.** If something is missing, add it to
`CLAUDE.md`.

## Domain-specific rules

- **Finanças** — `models/marts/financas/_docs_financas.md` is the single source of truth for
  spending categories, investment layers and the investment policy. Change the rule there, then
  propagate to the duplicated constants in
  `.claude/skills/relatorio-financas/scripts/montar_relatorio.py`.
- **PostgreSQL only** — date arithmetic is `- interval '3 days'`, never `dateadd()`.
- **Never hardcode schemas** — use `{{ ref('stg_xxx') }}` and
  `{{ source('raw', 'xxx') }}`; schema routing is handled by `dbt_project.yml` and the
  `generate_schema_name` macro.

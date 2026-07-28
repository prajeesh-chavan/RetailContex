# Development Guidelines

## Branching Strategy

- `main` — production. Only merged from `develop`.
- `develop` — integration. Only merged from feature/fix/chore branches.
- Feature branches — all work happens here. **No direct commits to `main` or `develop` ever.**

### Branch Naming

| Prefix       | When to use                         | Example                      |
|--------------|--------------------------------------|------------------------------|
| `feature/`   | New entities or features             | `feature/orders-silver`      |
| `fix/`       | Bug fixes                            | `fix/schema-date-cast`       |
| `chore/`     | Config, deps, tooling, docs          | `chore/add-dbt-packages`     |

## Workflow

1. `git checkout develop && git pull && git checkout -b <prefix>/<description>`
2. Implement changes with conventional commits (see below)
3. `git push -u origin <prefix>/<description>`
4. Create PR into `develop` via GitHub CLI or UI
5. Merge via GitHub UI (squash merge recommended)
6. Delete the remote branch after merge

## Commit Message Convention

```
<type>(<scope>): <description>
```

| Type       | Usage                             | Example                                  |
|------------|-----------------------------------|------------------------------------------|
| `feat`     | New entity, feature, or component | `feat(orders): add bronze pipeline`       |
| `fix`      | Bug fix                           | `fix(dbt): replace deprecated macro`      |
| `refactor` | Restructuring without new behavior | `refactor(silver): extract generic runner`|
| `chore`    | Config, deps, tooling, docs       | `chore(makefile): add dbt-deps target`    |
| `test`     | Adding or updating tests          | `test(customer): add schema unit tests`   |

## Coding Conventions per Entity

Every new entity follows the same template:

1. `src/schemas/<entity>_schema.py` — source Kafka schema + bronze Parquet schema
2. `src/bronze/<entity>.py` — 3-5 line runner call
3. `src/silver/<entity>.py` — transform function + runner call
4. `dbt_retail/models/silver/sources.yml` — add table entry under `snowflake_silver`
5. `dbt_retail/models/gold/dim_<entity>.sql` — incremental merge model
6. `dbt_retail/models/gold/dim_<entity>.yml` — column tests

Minimal boilerplate: schemas + transform function ~60 lines per entity.

## Order of Operations

- Bronze → Silver → dbt run → dbt test (must run in sequence)
- Delete checkpoint dirs after schema column renames (checkpoint stores column metadata)

## Versioning

Tags are annotated (`git tag -a`) with a description of what was delivered.

| Tag | Trigger | What It Signifies |
|-----|---------|-------------------|
| `v0.1.0` | Phase 4 merged | All 30 entities — bronze → silver → gold operational |
| `v0.2.0` | CI/CD + tests merged | ruff lint, pytest (152), dbt run/test on every PR |
| `v0.3.0` | Docker + Dagster + dbt-docs merged | Containerized, scheduled, data catalog live |
| `v1.0.0` | Monitoring + secrets + rollback | Production-hardened — survive real outages |

### Future roadmap

| Tag | Milestone | Scope |
|-----|-----------|-------|
| `v2.0.0` | Feature store | Point-in-time features from gold layer |
| `v2.1.0` | Training pipeline | Auto feature engineering + experiment tracking |
| `v2.2.0` | Model registry | Register, version, batch inference |
| `v2.3.0` | Feature serving | Real-time feature lookup for online inference |
| `v3.0.0` | Anomaly detection | Streaming outlier detection on bronze |
| `v3.1.0` | Recommendations | Product/cross-sell engine |
| `v3.2.0` | Forecasting | Demand, inventory, revenue prediction |
| `v3.3.0` | Real-time decisions | Inference API + A/B testing |

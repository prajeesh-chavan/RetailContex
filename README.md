# RetailCortex

Enterprise medallion pipeline for retail analytics — streaming CDC ingestion, lakehouse transformation, and BI-ready dimensional models on Snowflake.

![CI](https://github.com/prajeesh-chavan/RetailCortex/actions/workflows/ci.yml/badge.svg)
![dbt tests](https://img.shields.io/badge/dbt_tests-264-FF694B)
![Python](https://img.shields.io/badge/python-3.10-blue)
![Snowflake](https://img.shields.io/badge/snowflake-powered-00BFFF)

---

## Architecture

```
                         RETAILCORTEX MEDALLION PIPELINE

  KAFKA (CDC)              BRONZE                   SILVER                   GOLD
  ───────────             ──────                   ──────                   ────
  30 topics      ───▶    PySpark Streaming  ───▶  PySpark Batch    ───▶   dbt Models
  Debezium CDC            Parse JSON              Clean & Transform         Dimensions
                          Parquet + DLQ           Snowflake Views           Fact Tables
                          Partition by date       Incremental Merge         BI-Ready
                               │                        │                       │
                               │                        │                       │
                         ┌─────▼─────┐          ┌───────▼───────┐        ┌──────▼──────┐
                         │ DLQ       │          │ Silver DLQ    │        │ dbt Test    │
                         │ Failed    │          │ Failed        │        │ 264 Checks  │
                         │ Records   │          │ Records       │        │ not_null    │
                         └───────────┘          └───────────────┘        │ unique      │
                                                                         │ relations   │
                                                                         └─────────────┘

                          ORCHESTRATION ║ CI/CD
                          ─────────────────────
                          Dagster (scheduled daily)
                          GitHub Actions: lint → pytest (152) → dbt run → dbt test
```

## Tech Stack

| Layer                       | Technology                                      |
|-----------------------------|-------------------------------------------------|
| **Ingestion**               | Kafka, Debezium CDC, PySpark Structured Streaming |
| **Bronze Storage**          | Parquet (date-partitioned)                      |
| **Silver Transformation**   | PySpark + Snowflake Views                       |
| **Gold Modeling**           | dbt (incremental merge, dimensions + facts)      |
| **Data Quality**            | dbt test — 264 automated checks                 |
| **Orchestration**           | Dagster (daily schedule)                        |
| **CI/CD**                   | GitHub Actions (ruff → pytest → dbt)            |
| **Data Catalog**            | dbt docs (GitHub Pages)                         |

## Key Features

### Streaming Ingestion from 30 CDC Topics
PySpark Structured Streaming reads 30 Kafka CDC topics in parallel. Each entity has a dedicated streaming query with checkpointing, schema validation via `from_json`, and a dead-letter queue for parse failures. Bronze data lands as date-partitioned Parquet.

### Medallion Architecture
- **Bronze** — raw CDC payload as-is, plus Kafka metadata (topic, partition, offset, ingestion timestamp)
- **Silver** — cleaned, typed, deduplicated views in Snowflake with merge-based upserts
- **Gold** — dimensional models (dim_customer, dim_product, etc.) and fact tables (fact_orders, fact_inventory, etc.) built by dbt

### Production-Grade Data Quality
264 dbt tests run automatically on every PR and in production: `not_null` on primary keys, `unique` on natural keys, `relationships` across foreign keys, `accepted_values` on status enums.

### CI/CD Pipeline
Every pull request goes through ruff lint → pytest (152 schema & config tests) → dbt deps → dbt run → dbt test. Failures block merge.

### Dagster Orchestration
Daily scheduled pipeline runs the full bronze → silver → dbt sequence. Asset-based model maps naturally to the medallion structure.

## Entity Model — 30 Entities, 6 Domains

| Domain             | Entities                                                                       |
|--------------------|--------------------------------------------------------------------------------|
| **Master Data**    | customer, product, category, brand, product_variant, sales_channel, carrier, address, vendor, warehouse |
| **Relations**      | customer_address, vendor_address                                               |
| **Orders**         | order, order_item, order_address, order_status_history, order_item_discount    |
| **Inventory**      | inventory, inventory_movement                                                  |
| **Payments & Logistics** | payment, refund, shipment, shipment_item, returns, return_item            |
| **Engagement**     | cart, cart_item, product_review, customer_event                                |

## Getting Started

```bash
# 1. Configure
cp config/kafka.yaml.example config/kafka.yaml
cp config/snowflake.yaml.example config/snowflake.yaml
# Edit configs with your credentials

# 2. Run a single entity
python run_pipeline.py --entities customer --bronze-timeout 120

# 3. Run all entities in parallel
python run_pipeline.py --parallel 4 --bronze-timeout 120
```

## Project Structure

```
├── src/
│   ├── bronze/          # Per-entity streaming runners
│   ├── silver/          # Per-entity transform + Snowflake upsert
│   ├── schemas/         # 30 entity schemas (StructType)
│   └── common/          # Spark session, config, reader, writer, DLQ, paths
├── dbt_retail/
│   ├── models/
│   │   ├── silver/      # Silver source definitions
│   │   └── gold/        # Dimensional + fact models
│   └── tests/           # Custom dbt tests
├── dagster/             # Dagster definitions + schedules
├── config/              # Kafka + Snowflake YAML configs
├── tests/               # pytest unit tests (152)
├── .github/workflows/   # CI/CD
└── run_pipeline.py      # Orchestrator
```

## Dashboard Preview

| Gold Model             | Type             | Grain                   |
|------------------------|------------------|-------------------------|
| `dim_customer`         | Slowly Changing  | customer_id             |
| `dim_product`          | Slowly Changing  | variant_id              |
| `fact_orders`          | Transactional    | order_id                |
| `fact_inventory`       | Snapshot         | inventory_id            |
| `customer_360`         | Aggregated       | customer_id             |
| `daily_sales`          | Aggregated       | sale_date + channel     |
| `product_performance`  | Aggregated       | product + date          |
| `inventory_health`     | Aggregated       | warehouse + product     |
| `channel_performance`  | Aggregated       | channel + date          |

## CI/CD Pipeline

```
Pull Request ──▶ ruff lint ──▶ pytest (152) ──▶ dbt deps ──▶ dbt run ──▶ dbt test ──▶ Merge
                                                                    │
                                                                    └── GitHub Pages (docs)
```

## License

MIT

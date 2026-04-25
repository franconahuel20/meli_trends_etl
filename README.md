# Mercado Libre Trends Analytics Platform

## 1. Overview

This project implements a fully containerized analytics platform for Mercado Libre search trends. It extracts trends from the Mercado Libre API, stores raw files in MinIO, standardizes the data, applies fuzzy matching against brand and character reference tables in PostgreSQL, builds a final analytical model with dbt, and exposes the result in Apache Superset for dashboarding.

The platform is designed around a professional data engineering workflow:

```text
PostgreSQL seed tables
        ↓
Airflow orchestration
        ↓
Mercado Libre API extraction
        ↓
MinIO L1 raw CSV
        ↓
MinIO L2 clean parquet
        ↓
MinIO L3 standardized parquet + PostgreSQL staging table
        ↓
dbt final analytical table
        ↓
Superset dashboards
```

The main final table is:

```text
mdm_revenue_research_meli.trends
```

This is the table that Superset should use for the visualizations.

---

## 2. Stack

The project uses the following mandatory components:

| Component | Purpose |
|---|---|
| PostgreSQL | Stores reference tables, staging tables and the final dbt model |
| Apache Airflow | Orchestrates the end-to-end pipeline |
| dbt | Builds the final analytical table |
| Apache Superset | Visualizes the final model |
| MinIO S3 | Stores L1, L2 and L3 data lake files |
| Docker Compose | Runs the full stack locally |

---

## 3. Project structure

```text
meli_trends_stack/
├── dags/
│   └── meli_trends_end_to_end.py
├── data/
│   └── input/
│       ├── brands.xlsx
│       ├── characters.xlsx
│       └── mercado_libre_subcat.xlsx
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── schema.yml
│   │   └── trends.sql
│   └── profiles/
│       └── profiles.yml
├── docker/
│   ├── airflow/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── superset/
│       └── Dockerfile
├── scripts/
│   └── init_postgres.sql
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## 4. Environment configuration

Create your local `.env` file:

```bash
cp .env.example .env
```

Then update the Mercado Libre secret:

```env
ML_GRANT_TYPE=client_credentials
ML_CLIENT_ID=3347009040757312
ML_CLIENT_SECRET=your_real_secret_here
```

The project intentionally provides `.env.example` instead of hardcoding secrets in the repository. Do not commit `.env` to Git.

---

## 5. Running the full platform

From the project root, run:

```bash
docker compose up --build
```

When all services are ready:

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | postgres / postgres |

---

## 6. Airflow DAG

The main DAG is:

```text
meli_trends_end_to_end
```

It runs every day at:

```text
23:00 America/Argentina/Buenos_Aires
```

You can also trigger it manually from the Airflow UI.

### DAG tasks

| Task | Description |
|---|---|
| `load_excel_inputs_to_postgres` | Loads the three Excel files into PostgreSQL reference tables |
| `get_meli_access_token` | Gets a Bearer access token from Mercado Libre |
| `extract_meli_trends_to_l1` | Calls the trends endpoint and saves a CSV into MinIO L1 |
| `transform_l1_to_l2` | Cleans and deduplicates raw data, then writes parquet into MinIO L2 |
| `fuzzy_match_l2_to_l3` | Applies exact and fuzzy matching against brands and characters |
| `run_dbt_models` | Runs dbt and creates the final analytical table |

---

## 7. PostgreSQL schemas and tables

The project creates the following schemas:

```sql
standardization_revenue_research_meli
stg_revenue_research_meli
mdm_revenue_research_meli
```

### Reference tables

```sql
standardization_revenue_research_meli.brands
standardization_revenue_research_meli.characters
standardization_revenue_research_meli.mercado_libre_subcat
```

### Staging table

```sql
stg_revenue_research_meli.meli_trends_l3
```

### Final table

```sql
mdm_revenue_research_meli.trends
```

---

## 8. MinIO data lake layout

The bucket is:

```text
meli-datalake
```

The data lake paths are:

```text
L1_delta/revenue_research_meli/trends/
L2_databases/revenue_research_meli/trends/
L3_standardization/revenue_research_meli/trends/
```

L1 contains CSV files with the format:

```text
mercado_libre_trends_ddmmyyyy.csv
```

L2 and L3 are stored as parquet files partitioned by date.

---

## 9. Fuzzy matching logic

The L3 task applies the following logic:

1. Normalize keywords:
   - lowercase
   - remove accents
   - remove punctuation
   - trim duplicated spaces

2. Match brands:
   - exact word match
   - fuzzy match with threshold 90

3. Match characters:
   - exact word match
   - fuzzy match with threshold 85

4. Enrich matched characters with:
   - owner group
   - TWDC flag
   - franchise level 1
   - franchise level 2
   - franchise level 3
   - content

5. Generate analytical flags:
   - `has_brand_str`
   - `has_character_str`
   - `has_franchise_n1_str`
   - `has_franchise_n2_str`
   - `has_franchise_n3_str`
   - `branded_str`

---

## 10. dbt model

The dbt model is:

```text
dbt/models/trends.sql
```

It creates the final table:

```sql
mdm_revenue_research_meli.trends
```

To run dbt manually inside the Airflow container:

```bash
docker exec -it meli-airflow-scheduler bash
cd /opt/airflow/dbt
dbt run --profiles-dir /opt/airflow/dbt/profiles
dbt test --profiles-dir /opt/airflow/dbt/profiles
```

---

## 11. Superset setup

Open Superset:

```text
http://localhost:8088
```

Login:

```text
admin / admin
```

### Add PostgreSQL database

Go to:

```text
Settings → Database Connections → + Database
```

Use this SQLAlchemy URI:

```text
postgresql+psycopg2://postgres:postgres@postgres:5432/snowflake_dev
```

Then create a dataset from:

```text
mdm_revenue_research_meli.trends
```

---

## 12. Dashboard guide

Create a dashboard named:

```text
Mercado Libre Trends Analytics
```

Use the dataset:

```text
mdm_revenue_research_meli.trends
```

### 12.1 Branded search trends by country

Chart type:

```text
Line Chart
```

Configuration:

| Field | Value |
|---|---|
| Time Column | `week_dte` |
| Metric | `COUNT(*)` |
| Dimension / Series | `country_str` |
| Filter | `branded_str = 'Branded'` |
| X Axis | `week_dte` |
| Y Axis | count |

Suggested title:

```text
Tendencias de búsquedas brandeadas por país
```

---

### 12.2 Share by company

Chart type:

```text
Treemap
```

Configuration:

| Field | Value |
|---|---|
| Dimension | `group_company_owner_rights_str` |
| Metric | `COUNT(*)` |
| Filter | `group_company_owner_rights_str IS NOT NULL` |

Suggested title:

```text
Share por Compañía
```

---

### 12.3 Companies by country

Chart type:

```text
Bar Chart
```

Configuration:

| Field | Value |
|---|---|
| X Axis | `country_str` |
| Metric | `COUNT(*)` |
| Series | `group_company_owner_rights_str` |
| Filter | `group_company_owner_rights_str IS NOT NULL` |
| Stack | enabled |

Suggested title:

```text
Compañías por País
```

---

### 12.4 Franchise level 1 share

Chart type:

```text
Pie Chart` or `Donut Chart
```

Configuration:

| Field | Value |
|---|---|
| Dimension | `franchise_n1_str` |
| Metric | `COUNT(*)` |
| Filter | `franchise_n1_str IS NOT NULL` |

Suggested title:

```text
Franquicias Nivel 1
```

---

### 12.5 Franchise level 2 by country

Chart type:

```text
Bar Chart
```

Configuration:

| Field | Value |
|---|---|
| X Axis | `country_str` |
| Metric | `COUNT(*)` |
| Series | `franchise_n2_str` |
| Filter | `franchise_n2_str IS NOT NULL` |
| Stack | enabled |

Suggested title:

```text
Franquicias Nivel 2 por país
```

---

### 12.6 Top 10 branded trends in a category

Chart type:

```text
Horizontal Bar Chart
```

Configuration:

| Field | Value |
|---|---|
| Dimension | `keyword_str` |
| Metric | `COUNT(*)` |
| Filter | `branded_str = 'Branded'` |
| Sort | count descending |
| Row limit | 10 |

Suggested title:

```text
Top 10 de Tendencias Branded en la categoría
```

---

### 12.7 Top 20 characters

Chart type:

```text
Word Cloud
```

Configuration:

| Field | Value |
|---|---|
| Text Column | `character_str` |
| Metric | `COUNT(*)` |
| Filter | `character_str IS NOT NULL` |
| Row limit | 20 |

Suggested title:

```text
Personajes - Top 20
```

---

### 12.8 Branded vs Unbranded

Chart type:

```text
Pie Chart / Donut Chart
```

Configuration:

| Field | Value |
|---|---|
| Dimension | `branded_str` |
| Metric | `COUNT(*)` |

Suggested title:

```text
Branded / Unbranded
```

---

### 12.9 Branded franchises

Chart type:

```text
Treemap
```

Configuration:

| Field | Value |
|---|---|
| Dimension | `franchise_n1_str` |
| Metric | `COUNT(*)` |
| Filter | `franchise_n1_str IS NOT NULL` |

Suggested title:

```text
Franquicias Brandeadas
```

---

### 12.10 Top 100 searched products

Chart type:

```text
Table
```

Configuration:

| Column | Value |
|---|---|
| Product | `nouns_keyword_str` |
| Metric | `COUNT(*)` |
| Sort | count descending |
| Row limit | 100 |

Suggested title:

```text
Top 100 de productos buscados
```

---

### 12.11 Branded product and character

Chart type:

```text
Table
```

Configuration:

| Column | Value |
|---|---|
| Character | `character_str` |
| Product | `nouns_keyword_str` |
| Metric | `COUNT(*)` |
| Sort | count descending |

Suggested title:

```text
Producto branded y personaje
```

---

### 12.12 Top 10 trends

Chart type:

```text
Bar Chart
```

Configuration:

| Field | Value |
|---|---|
| Dimension | `keyword_str` |
| Metric | `COUNT(*)` |
| Sort | count descending |
| Row limit | 10 |

Suggested title:

```text
Top 10 en Tendencias
```

---

### 12.13 Words searched inside franchise level 2

Chart type:

```text
Treemap
```

Configuration:

| Field | Value |
|---|---|
| Dimension | `nouns_keyword_str` |
| Metric | `COUNT(*)` |
| Filter | `franchise_n2_str IS NOT NULL` |
| Row limit | 20 |

Suggested title:

```text
Palabras buscadas dentro de franquicias Nivel 2 TOP 20
```

---

## 13. Recommended dashboard styling

To match the dashboard screenshots:

- Use a dark navy header for each chart title.
- Use white cards with subtle shadow.
- Use consistent Spanish titles.
- Use filters at dashboard level:
  - country
  - category
  - subcategory
  - date range
  - branded/unbranded
  - franchise level 1
  - franchise level 2
- Use `COUNT(*)` as the initial metric.
- Later, if the API provides a volume/rank field, replace `COUNT(*)` with a stronger business metric.

---

## 14. Operational notes

### Re-running a day

The current local version replaces the staging L3 table on each run. For production, use an incremental loading strategy with a merge key based on:

```text
keyword_str, url_str, date_dte, country_str, category_str, subcategory_str
```

### Secrets

Never commit real API secrets to Git. Keep them only in `.env`, Airflow Connections, or a secret manager.

### Scaling

For a larger number of subcategories, replace the sequential API loop with Airflow dynamic task mapping or a batch-per-country design.

---

## 15. Troubleshooting

### Airflow cannot connect to PostgreSQL

Check that the PostgreSQL container is healthy:

```bash
docker ps
docker logs meli-postgres
```

### Mercado Libre API returns 401

Validate:

```env
ML_CLIENT_ID
ML_CLIENT_SECRET
ML_GRANT_TYPE
```

### MinIO bucket missing

Run:

```bash
docker compose up create-minio-bucket
```

### dbt model does not exist in Superset

Run the Airflow DAG first, then verify:

```sql
SELECT COUNT(*) FROM mdm_revenue_research_meli.trends;
```

---

## 16. Professional next steps

Recommended improvements:

1. Add `brand_match_score` and `character_match_score`.
2. Convert the dbt model to incremental materialization.
3. Add dbt data quality tests for nulls, accepted values and uniqueness.
4. Add Great Expectations or Soda for raw data validation.
5. Add Superset dashboard exports for CI/CD.
6. Replace fuzzy matching with embeddings for semantic matching.

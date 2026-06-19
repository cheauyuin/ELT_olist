# ELT Olist — Brazilian E-Commerce Pipeline

An end-to-end ELT pipeline using the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.

## Tech Stack

| Tool | Purpose |
|---|---|
| Supabase (PostgreSQL) | Source system — holds raw CSV data |
| Meltano | Extract from Supabase, load into BigQuery |
| BigQuery | Data warehouse |
| dbt | Transformations, testing, star schema |
| Great Expectations | Raw layer data quality validation |
| Dagster | Orchestration — daily automated pipeline |

## Project Structure

```
ELT_olist/
├── m2-environment.yml          # Conda environment (includes all dependencies)
├── olist_meltano/              # Meltano EL pipeline (Supabase → BigQuery)
│   ├── meltano.yml
│   └── .env.example
├── olist_transform/            # dbt transformation layer
│   ├── profiles.yml
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/            # 8 staging views (one per source table)
│       └── marts/              # 10 mart models (star schema + analytical marts)
├── great_expectations/         # Raw layer data quality validation
│   └── ge_olist_raw.py
├── olist_dagster/              # Dagster orchestration
│   └── olist_dagster/
│       ├── assets/pipeline.py  # 8 pipeline assets
│       └── definitions.py
└── report/
    └── generate_dashboard.py   # Generates docs/index.html from BigQuery marts
```

## Data Model

The raw data consists of 9 tables loaded into BigQuery under the `olist_raw` dataset.

### Staging Layer (`olist_transformed_staging`)
8 views — one per source table. Light cleaning only: type casting, column renaming, zip code padding (Brazilian CEP codes are always 5 digits), and product category translation joined into `stg_products`.

### Marts Layer (`olist_transformed_marts`)

| Model | Description | Sample Questions |
|---|---|---|
| `dim_customers` | Customers enriched with lat/lng from geolocation | Customer distribution by state, repeat vs one-time buyers |
| `dim_products` | Products with English category name, photos, dimensions | Best performing categories, does photo count affect sales? |
| `dim_sellers` | Sellers enriched with lat/lng — reads from `snap_dim_sellers` (active record only) | Seller distribution by state, top sellers by revenue |
| `dim_sellers_history` | Full SCD Type 2 history of seller location changes | Point-in-time seller attribution |
| `fact_orders` | One row per order item. PK: `order_item_sk`. Includes `delivery_days`, `is_late` | Revenue by month/category/state, late delivery rate |
| `fact_reviews` | One row per review with `sentiment` derived from `review_score` | Average score by seller/product, delivery vs rating correlation |
| `mart_seller_health` | Composite seller health score (40% review, 35% on-time, 25% delivery rate) with 90-day trend window | Early warning for declining sellers |
| `mart_customer_summary` | One row per `customer_unique_id` with order history and churn segment | Repeat purchase rate, days since last order |
| `mart_rfm_scores` | RFM segmentation with deterministic NTILE scoring and campaign assignment | Champions, at-risk, lost segments |
| `mart_cohort_retention` | Monthly cohort retention rates | When do customers disengage? |

### Joining the marts

| Join | Column to use |
|---|---|
| `fact_orders` → `dim_customers` | `customer_id` |
| `fact_orders` → `dim_products` | `product_id` |
| `fact_orders` → `dim_sellers` | `seller_id` |
| `fact_reviews` → `fact_orders` | `order_id` |
| `fact_reviews` → `dim_customers` | `customer_id` |

> `order_item_sk` is the row identifier for `fact_orders` — use it as the PK in tests, not as a join key.

---

## Setup Instructions (For Group Members)

### Prerequisites
- [Anaconda](https://www.anaconda.com/download) or Miniconda installed
- A GitHub collaborator invite accepted from Marcus — required to push changes to the repo
- The service account JSON key file — ask the project owner (Marcus) to share it with you securely
- A Google account added to the GCP project by Marcus — required to view data and run queries in the BigQuery console

---

### For the project owner only — adding members to GitHub and GCP

> Skip this section if you are not the project owner. Send your GitHub username and Google account email to Marcus.

**GitHub — add as collaborator:**
1. Go to the GitHub repo → **Settings → Collaborators**
2. Click **Add people**, enter the group member's GitHub username
3. They will receive an email invite — they must accept it before they can push

**GCP — grant BigQuery access:**
1. Go to [GCP Console](https://console.cloud.google.com) and select the `olist-498903` project
2. Navigate to **IAM & Admin → IAM** → **Grant Access**
3. Enter the group member's Google account email, assign role **BigQuery User**, click **Save**

---

### Step 1 — Clone the repo

```bash
git clone <repo-url>
cd ELT_olist
```

### Step 2 — Create and activate conda environment

```bash
conda env create -f m2-environment.yml
conda activate m2
```

### Step 3 — Save BigQuery credentials

Save the service account JSON key file somewhere safe on your machine **outside the repo** (e.g. `~/.gcp/olist-key.json`). Note the full path.

> The key gives dbt programmatic access to create and update tables in BigQuery. To view and query data in the BigQuery console, your Google account must be added to the GCP project by Marcus (see above).

### Step 4 — Configure dbt

Open `olist_transform/profiles.yml` and update the `keyfile` path to where you saved your JSON key:
```yaml
keyfile: /YOUR/PATH/TO/your-key.json
```

### Step 5 — Install dbt packages

```bash
cd olist_transform
dbt deps
```

### Step 6 — Run dbt models

```bash
dbt run
```

### Step 7 — Run dbt tests

```bash
dbt test
```

> All dbt commands should be run from inside the `olist_transform/` directory.

---

## 🔄 Meltano — Extract & Load

Meltano extracts all 9 source tables from Supabase PostgreSQL and loads them into BigQuery `olist_raw`.

### Replication mode: FULL_TABLE with overwrite

We use `overwrite: true` in `target-bigquery`, which replaces the entire raw table on every run rather than appending. This keeps `olist_raw` clean and prevents row accumulation across runs.

**Why not INCREMENTAL?**

Meltano's INCREMENTAL mode requires a replication key — a column in the source table that increases monotonically with each change (e.g. `updated_at`). The Olist source tables were loaded from static Kaggle CSVs and have no such business timestamp. The only available candidate, `_sdc_sequence`, is a metadata field stamped by `tap-postgres` at extraction time — it does not exist in the source schema and cannot be used as a replication key.

### Running Meltano

The data is already loaded into BigQuery. You do not need to run Meltano unless you want to reload the raw data from scratch.

If you do need to re-run:
1. Copy `olist_meltano/.env.example` to `olist_meltano/.env` and fill in your Supabase connection string
2. Update `credentials_path` in `olist_meltano/meltano.yml` to your local key file path
3. Run from inside `olist_meltano/`:
```bash
meltano run tap-postgres target-bigquery
```

---

## ✅ Great Expectations — Data Quality Validation

`ge_olist_raw.py` validates all 9 raw tables before dbt transformation. It runs 7 suites covering 44 structural expectations (not_null, unique, accepted_values, row count bounds, value ranges) plus 20 cross-table anomaly checks (orphan records, time inversions, duplicate reviews, geolocation coverage gaps).

If any of the 44 GE suite expectations fail, the script exits with code 1 — Dagster treats this as an asset failure and halts the pipeline before dbt runs. The anomaly checks are informational and do not halt the pipeline.

To run manually:
```bash
conda activate m2
cd ~/ELT_olist
python great_expectations/ge_olist_raw.py
```

---

## ⚡ Dagster — Orchestration

Dagster orchestrates the full pipeline as 8 assets connected via `deps=[]`. If any asset fails, all downstream assets are skipped automatically.

```
meltano_extract_load
        ↓
ge_raw_validation
        ↓
   dbt_staging
        ↓
  dbt_snapshot
        ↓
   dbt_marts
        ↓
   ┌────┴────────────────┐
   ↓                      ↓
generate_dashboard   alert_declining_sellers
   ↓
git_push_dashboard
```

| Asset | What it does |
|---|---|
| `meltano_extract_load` | tap-postgres → BigQuery `olist_raw` |
| `ge_raw_validation` | GE gate — halts pipeline if any expectation fails |
| `dbt_staging` | `dbt build --select staging` (includes dbt tests) |
| `dbt_snapshot` | `dbt snapshot` — SCD Type 2 for `snap_dim_sellers` |
| `dbt_marts` | `dbt build --select marts` (includes dbt tests) |
| `alert_declining_sellers` | Emails alert if any seller `trend_status != 'stable'` |
| `generate_dashboard` | `python report/generate_dashboard.py` → `docs/index.html` |
| `git_push_dashboard` | `git add` + commit + push → GitHub Pages auto-deploys |

`alert_declining_sellers` and `generate_dashboard` both depend only on `dbt_marts` and run in parallel — a failure in one does not block the other.

Schedule: daily at **SGT 02:00** (UTC 18:00).

### Setup

```bash
# One-time install
conda activate m2
cd olist_dagster
pip install -e ".[dev]"

# Parse dbt manifest (required before first run)
cd ../olist_transform
dbt deps && dbt parse

# Start Dagster UI
cd ../olist_dagster
dagster dev
# → open http://localhost:3000
```

### Environment variables

Create a `.env` file at the repo root (never commit this file):
```
GMAIL_ADDRESS=your-gmail@gmail.com
GMAIL_APP_PASSWORD=your-app-password
ALERT_RECIPIENT=your-gmail@gmail.com
```

Generate a Gmail App Password at: Google Account → Security → 2-Step Verification → App Passwords.

### Dashboard

The pipeline auto-generates `docs/index.html` and pushes it to GitHub Pages on every run.

To enable GitHub Pages: repo Settings → Pages → Source: `main` branch, folder: `/docs`.

Live dashboard: `https://<github-username>.github.io/ELT_olist/`

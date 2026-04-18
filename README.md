# Travel Analytics — dbt Pipeline

**SaaS Travel & Expense Analytics — Modern Data Stack**

A production-grade Analytics Engineering portfolio project demonstrating a full dbt pipeline on travel and expense data. Built with dbt, DuckDB, Python, Streamlit, and GitHub Actions CI/CD.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│  bookings.csv   expenses.csv   employees.csv   companies.csv           │
│  (5,000 rows)   (8,000 rows)   (500 rows)      (50 rows)               │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │  Python data generation (seed=42)
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DuckDB (local) / Snowflake (prod)                   │
│                         Raw tables loaded from CSV                     │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │  dbt layers │
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌─────────────┐  ┌────────────┐  ┌─────────────┐
   │   STAGING   │  │INTERMEDIATE│  │    MARTS    │
   │  (views)    │  │  (views)   │  │  (tables)   │
   │             │  │            │  │             │
   │ stg_bookings│  │int_bookings│  │fct_bookings │
   │ stg_expenses│  │_enriched   │  │fct_expenses │
   │ stg_employees  │int_expenses│  │dim_employees│
   │ stg_companies  │_enriched   │  │dim_companies│
   └─────────────┘  └────────────┘  └──────┬──────┘
                                           │
                                    ┌──────▼──────┐
                                    │  Streamlit  │
                                    │  Dashboard  │
                                    └─────────────┘
```

## dbt Model Lineage

```
stg_bookings  ──┐
stg_employees ──┼──► int_bookings_enriched ──► fct_bookings
stg_companies ──┘

stg_expenses  ──┐
stg_employees ──┼──► int_expenses_enriched ──► fct_expenses
stg_companies ──┘

stg_employees ──► dim_employees
stg_companies ──► dim_companies
```

**Staging** — thin rename + cast layer, no business logic
**Intermediate** — joins, FX conversion, derived flags (is_cancelled, booking_month)
**Marts** — analytics-ready fact & dimension tables, materialized as tables

---

## Quick Start

### Prerequisites

- Python 3.12+
- Git

### 1. Clone & install dependencies

```bash
git clone https://github.com/TwinAnalytics/travel-analytics-dbt.git
cd travel-analytics-dbt
pip install -r requirements.txt
```

### 2. Generate synthetic data

```bash
python data/generate_data.py
```

This creates 4 CSV files in `data/`:
- `bookings.csv` (5,000 rows)
- `expenses.csv` (8,000 rows)
- `employees.csv` (500 rows)
- `companies.csv` (50 rows)

### 3. Run dbt pipeline

```bash
cd dbt_project

# Install dbt packages
dbt deps

# Load CSVs into DuckDB + run all models
DATA_DIR="$(pwd)/../data" dbt run --profiles-dir .

# Run all data quality tests
DATA_DIR="$(pwd)/../data" dbt test --profiles-dir .
```

Expected output: 11 models pass, 113 tests pass (0 errors).

### 4. Launch the dashboard

```bash
cd ..
streamlit run dashboard/app.py
```

Navigate to [http://localhost:8501](http://localhost:8501)

---

## Project Structure

```
travel-analytics-dbt/
├── README.md
├── requirements.txt
│
├── data/
│   └── generate_data.py          # Synthetic data generation (NumPy seed=42)
│
├── dbt_project/
│   ├── dbt_project.yml           # dbt project config (DuckDB adapter)
│   ├── profiles.yml              # Connection profile (local DuckDB)
│   ├── packages.yml              # dbt_utils dependency
│   │
│   ├── models/
│   │   ├── staging/              # Raw → typed & renamed (views)
│   │   ├── intermediate/         # Business logic + joins (views)
│   │   └── marts/                # Analytics-ready tables (tables)
│   │
│   ├── tests/
│   │   └── assert_spend_positive.sql  # Singular data quality test
│   │
│   └── macros/
│       └── cents_to_euros.sql    # Reusable currency conversion macro
│
├── dashboard/
│   └── app.py                    # 4-page Streamlit dashboard
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml            # GitHub Actions: CI on push/PR
│
└── docs/
    └── project_overview.md       # Full architecture documentation
```

---

## dbt Tests

Every model has YAML-defined schema tests:

| Test type         | Applied to                                      |
|-------------------|-------------------------------------------------|
| `unique`          | All primary keys (booking_id, expense_id, etc.) |
| `not_null`        | All primary keys + critical columns             |
| `accepted_values` | status, trip_type, category, seniority, tier    |
| `relationships`   | fct_bookings/fct_expenses → dim_employees/companies |

Plus a **singular test** (`assert_spend_positive.sql`) that fails if any booking has `amount_cents <= 0`.

---

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/dbt_ci.yml`) runs on every push/PR to `main`:

1. **Checkout** — clone the repository
2. **Python setup** — install Python 3.12 + cache pip
3. **Install deps** — `pip install -r requirements.txt`
4. **Generate data** — `python data/generate_data.py`
5. **dbt deps** — install dbt packages
6. **dbt run** — run all models
7. **dbt test** — run all schema + singular tests
8. **dbt docs generate** — generate documentation
9. **Upload artifacts** — manifest, run_results, catalog

---

## Design Decisions

- **DuckDB locally, Snowflake in production** — DuckDB requires zero infrastructure for development and testing, while Snowflake provides the scalability needed for production workloads.
- **Cents as integers** — monetary amounts are stored as integer cents to avoid floating-point precision issues. The `cents_to_euros` macro handles conversion at query time.
- **Fixed FX rates** — EUR/GBP/USD conversion uses fixed rates (GBP=1.17, USD=0.92). In production, these would come from a daily FX rate lookup table.
- **SCD Type 0 for employees** — employee history is not tracked in this iteration. In production, SCD Type 2 would be appropriate for departments/seniority changes.
- **Denormalized marts** — employee and company attributes are denormalized into fact tables for query performance, avoiding runtime joins in BI tools.

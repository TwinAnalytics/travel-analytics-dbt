# Project Overview: Perk SaaS Travel & Spend Analytics

## 1. Purpose

This project demonstrates a production-grade analytics engineering workflow for a SaaS travel management platform modelled after Perk (formerly TravelPerk). It covers the full analytics engineering lifecycle: data ingestion, transformation, testing, documentation, CI/CD, and visualisation.

---

## 2. Dataset Design

All data is synthetically generated using NumPy (seed=42) to ensure full reproducibility. The schema mirrors realistic data structures from a corporate travel management platform.

### 2.1 companies (50 rows)
Represents customer companies using the Perk platform.

| Column | Type | Description |
|---|---|---|
| company_id | VARCHAR | Primary key (C0001–C0050) |
| company_name | VARCHAR | Company name |
| industry | VARCHAR | Industry vertical (10 options) |
| size_tier | VARCHAR | SMB / Mid-Market / Enterprise |
| country | VARCHAR | HQ country |
| contract_start | DATE | Perk contract start date |
| monthly_mrr_eur | NUMERIC | Monthly Recurring Revenue in EUR |

MRR ranges by tier: SMB €500–2K, Mid-Market €2K–10K, Enterprise €10K–50K.

### 2.2 employees (500 rows)
Employees at customer companies. Weighted toward larger companies.

| Column | Type | Description |
|---|---|---|
| employee_id | VARCHAR | Primary key (E00001–E00500) |
| company_id | VARCHAR | FK to companies |
| department | VARCHAR | Engineering / Sales / Marketing / Finance / HR / Operations / Product / Legal |
| seniority | VARCHAR | junior / mid / senior / manager / director |
| country | VARCHAR | Employee base country |
| start_date | DATE | Employment start date |
| is_active | BOOLEAN | Currently active flag (88% active) |

### 2.3 bookings (5,000 rows)
Core travel booking events — flights, hotels, trains, car rentals.

| Column | Type | Description |
|---|---|---|
| booking_id | VARCHAR | Primary key (B000001–B005000) |
| employee_id | VARCHAR | FK to employees |
| company_id | VARCHAR | FK to companies |
| booking_date | DATE | Date booking was made |
| travel_date | DATE | Date of travel/check-in |
| destination_city | VARCHAR | One of 25 major cities |
| destination_country | VARCHAR | Country of destination |
| trip_type | VARCHAR | flight (45%) / hotel (30%) / train (18%) / car (7%) |
| amount_cents | INTEGER | Booking cost in cents |
| currency | VARCHAR | EUR (55%) / USD (30%) / GBP (15%) |
| status | VARCHAR | confirmed (75%) / cancelled (15%) / pending (10%) |
| advance_days | INTEGER | Days between booking and travel (0–59) |
| policy_compliant | BOOLEAN | Compliance with travel policy (82% compliant) |

### 2.4 expenses (8,000 rows)
Employee expense submissions linked to travel activities.

| Column | Type | Description |
|---|---|---|
| expense_id | VARCHAR | Primary key (X000001–X008000) |
| employee_id | VARCHAR | FK to employees |
| company_id | VARCHAR | FK to companies |
| expense_date | DATE | Date expense was incurred |
| category | VARCHAR | meals / transport / accommodation / entertainment / other |
| amount_cents | INTEGER | Expense amount in cents |
| currency | VARCHAR | EUR / USD / GBP |
| status | VARCHAR | approved (70%) / rejected (15%) / pending (15%) |
| receipt_attached | BOOLEAN | Receipt provided (85%) |
| policy_compliant | BOOLEAN | Policy compliance (80%) |

---

## 3. dbt Architecture

The project follows a three-layer dbt architecture with clear separation of concerns.

### 3.1 Staging Layer (`models/staging/`)

**Purpose**: Thin interface with raw data. Only typing, renaming, and null filtering.

**Materialisation**: Views (no storage cost, always fresh)

**Design principles**:
- One staging model per source table
- Only `CAST` and column renames — no business logic
- Filter rows with null primary keys
- Named with `stg_` prefix

**Models**:
- `stg_bookings.sql` — casts booking_date/travel_date to DATE, amount_cents to INTEGER
- `stg_expenses.sql` — casts expense_date to DATE, booleans from CSV strings
- `stg_employees.sql` — casts start_date to DATE, is_active to BOOLEAN
- `stg_companies.sql` — casts contract_start to DATE, monthly_mrr_eur to NUMERIC

### 3.2 Intermediate Layer (`models/intermediate/`)

**Purpose**: Business logic and joins. Joins staging models together, applies calculations.

**Materialisation**: Views (logic tested without storage overhead)

**Design principles**:
- Not exposed to end users or BI tools
- Named with `int_` prefix
- Contains FX conversion, derived flags, time period columns

**Models**:
- `int_bookings_enriched.sql`:
  - Joins bookings + employees + companies
  - Adds `booking_month` (date_trunc to month)
  - Derives `is_cancelled` boolean flag
  - Computes `cost_eur` using the `cents_to_euros` macro + FX rates
- `int_expenses_enriched.sql`:
  - Joins expenses + employees + companies
  - Adds `expense_month`
  - Computes `cost_eur`

**FX Rate logic** (fixed rates, suitable for analytical purposes):
```sql
CASE currency
    WHEN 'GBP' THEN 1.17
    WHEN 'USD' THEN 0.92
    ELSE 1.00  -- EUR
END
```

### 3.3 Marts Layer (`models/marts/`)

**Purpose**: Analytics-ready, consumer-facing tables. Denormalized for BI performance.

**Materialisation**: Tables (computed once, fast reads)

**Design principles**:
- Named with `fct_` (fact) or `dim_` (dimension) prefix
- Employee/company attributes denormalized into facts for join-free BI
- All derived business metrics computed here

**Fact Tables**:
- `fct_bookings.sql`:
  - Grain: one row per booking
  - Adds `advance_booking_tier` (last_minute / standard / advance)
  - Adds `is_policy_compliant` (renamed from policy_compliant)
- `fct_expenses.sql`:
  - Grain: one row per expense submission

**Dimension Tables**:
- `dim_employees.sql`:
  - SCD Type 0 (snapshot at load time)
  - Adds `tenure_days` (days since start_date)
  - Denormalizes company_name, size_tier, industry
- `dim_companies.sql`:
  - Adds `contract_age_days` (days since contract start)
  - Adds `mrr_tier` bucket (Low / Mid / High / Premium)

---

## 4. Data Quality Tests

### 4.1 Schema Tests (YAML-defined)

Applied across all three layers:

| Test | Models | Columns |
|---|---|---|
| `unique` | All | Primary keys |
| `not_null` | All | PKs + critical fields |
| `accepted_values` | Staging + Marts | status, trip_type, category, seniority, size_tier, currency, advance_booking_tier, mrr_tier |
| `relationships` | Marts | fct_bookings.employee_id → dim_employees.employee_id |
| `relationships` | Marts | fct_bookings.company_id → dim_companies.company_id |
| `relationships` | Marts | fct_expenses.employee_id → dim_employees.employee_id |
| `relationships` | Marts | fct_expenses.company_id → dim_companies.company_id |
| `relationships` | Marts | dim_employees.company_id → dim_companies.company_id |

### 4.2 Singular Tests (`tests/`)

- `assert_spend_positive.sql` — fails if any booking has `amount_cents <= 0`
  - Rationale: negative amounts would indicate a data pipeline error (refunds are tracked as cancelled status, not negative values)

---

## 5. Macros

### `cents_to_euros(column)`
```sql
{% macro cents_to_euros(column) %}
    round(cast({{ column }} as numeric) / 100.0, 2)
{% endmacro %}
```

Used in both intermediate models to standardize the cents-to-euros conversion. Ensures consistent rounding to 2 decimal places across the entire pipeline.

---

## 6. CI/CD Pipeline

### GitHub Actions (`.github/workflows/dbt_ci.yml`)

Triggers on: push or PR to `main` or `develop`

**Steps**:
1. Checkout code
2. Set up Python 3.12
3. Install all requirements (dbt-duckdb, streamlit, etc.)
4. Generate synthetic CSV data
5. `dbt deps` — install dbt_utils package
6. `dbt run` — run all models against a CI DuckDB instance
7. `dbt test` — run all schema + singular tests
8. `dbt docs generate` — generate catalogue
9. Upload artifacts (manifest.json, run_results.json, catalog.json)

**Philosophy**: Every commit must pass the full test suite before merging to main. This prevents regressions in data transformations and ensures data contracts (schema tests) are always satisfied.

---

## 7. Dashboard Design

The Streamlit dashboard (`dashboard/app.py`) provides four analytical views:

### Page 1: Executive Overview
- KPI cards: total bookings, spend, avg value, policy compliance, total expenses, approval rate
- Booking spend by company tier (bar chart)
- Expense spend by category (donut chart)
- Monthly booking spend trend (area chart)

### Page 2: Travel Analytics
- KPI cards: confirmed rate, cancellation rate, avg advance days, flight share
- Monthly bookings trend (bar chart)
- Trip type mix (donut chart)
- Top 10 destination cities by booking volume (horizontal bar)
- Advance booking tier distribution (grouped bar)
- Policy compliance heatmap by department × trip type

### Page 3: Expense Analytics
- KPI cards: total expenses, total spend, receipt rate, policy compliance
- Stacked spend by department & category
- Approval status distribution (donut)
- Policy compliance by seniority (bar)
- Monthly expense trend (line chart)
- Receipt attachment rate table

### Page 4: Company Health
- KPI cards: total companies, total MRR, avg MRR, enterprise count
- MRR by company tier
- Bookings per company (top 20)
- MRR vs. booking spend scatter plot
- Top 10 companies by total spend table

---

## 8. Technology Choices

| Technology | Role | Production Alternative |
|---|---|---|
| DuckDB | Local analytical database | Snowflake |
| dbt-duckdb | dbt adapter for DuckDB | dbt-snowflake |
| dbt_utils | Utility macros (surrogate keys, etc.) | Same |
| Python / NumPy | Data generation | Airbyte / Fivetran |
| Streamlit | Dashboard | Tableau / Looker / Metabase |
| GitHub Actions | CI/CD | CircleCI / dbt Cloud |

---

## 9. Potential Production Enhancements

1. **Incremental models** — Add `is_incremental()` logic to fact tables to avoid full reprocessing on each run
2. **Dynamic FX rates** — Replace hardcoded rates with a `ref('fx_rates')` lookup table updated daily
3. **SCD Type 2 employees** — Track department/seniority changes over time using dbt snapshots
4. **Airflow orchestration** — Schedule daily runs: ingest CSVs → dbt run → dbt test → alert on failures
5. **dbt Exposures** — Document the Streamlit dashboard as a dbt exposure to track lineage end-to-end
6. **Data contracts** — Add `constraints` to mart models (Snowflake primary/foreign key enforcement)
7. **Monitoring** — Add `dbt-expectations` package for distribution tests (value ranges, completeness ratios)

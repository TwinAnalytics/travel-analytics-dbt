/*
  int_expenses_enriched
  ---------------------
  Joins expenses with employee and company context.
  Applies business logic:
    - Adds expense_month for time-series analysis
    - Converts amount to EUR using fixed FX rates
*/

with

expenses as (
    select * from {{ ref('stg_expenses') }}
),

employees as (
    select
        employee_id,
        department,
        seniority,
        country       as employee_country
    from {{ ref('stg_employees') }}
),

companies as (
    select
        company_id,
        company_name,
        industry,
        size_tier,
        country       as company_country,
        monthly_mrr_eur
    from {{ ref('stg_companies') }}
),

enriched as (
    select
        -- expense identifiers
        x.expense_id,
        x.employee_id,
        x.company_id,

        -- employee context
        e.department,
        e.seniority,
        e.employee_country,

        -- company context
        c.company_name,
        c.industry,
        c.size_tier,
        c.company_country,
        c.monthly_mrr_eur,

        -- expense dates
        x.expense_date,
        date_trunc('month', x.expense_date) as expense_month,

        -- expense details
        x.category,
        x.amount_cents,
        x.currency,
        x.status,
        x.receipt_attached,
        x.policy_compliant,

        -- currency conversion to EUR
        {{ cents_to_euros('x.amount_cents') }} *
            case x.currency
                when 'GBP' then 1.17
                when 'USD' then 0.92
                else 1.00
            end as cost_eur

    from expenses x
    left join employees e
        on x.employee_id = e.employee_id
    left join companies c
        on x.company_id = c.company_id
)

select * from enriched

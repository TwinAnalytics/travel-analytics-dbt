/*
  int_bookings_enriched
  ---------------------
  Joins bookings with employee and company context.
  Applies business logic:
    - Adds booking_month for time-series analysis
    - Derives is_cancelled flag
    - Converts amount to EUR using fixed FX rates:
        GBP -> EUR: 1.17
        USD -> EUR: 0.92
        EUR -> EUR: 1.00
*/

with

bookings as (
    select * from {{ ref('stg_bookings') }}
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
        -- booking identifiers
        b.booking_id,
        b.employee_id,
        b.company_id,

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

        -- booking dates
        b.booking_date,
        b.travel_date,
        date_trunc('month', b.booking_date) as booking_month,

        -- location
        b.destination_city,
        b.destination_country,

        -- booking details
        b.trip_type,
        b.amount_cents,
        b.currency,
        b.status,
        b.advance_days,
        b.policy_compliant,

        -- derived flags & amounts
        case when b.status = 'cancelled' then true else false end as is_cancelled,

        -- currency conversion to EUR
        {{ cents_to_euros('b.amount_cents') }} *
            case b.currency
                when 'GBP' then 1.17
                when 'USD' then 0.92
                else 1.00
            end as cost_eur

    from bookings b
    left join employees e
        on b.employee_id = e.employee_id
    left join companies c
        on b.company_id = c.company_id
)

select * from enriched

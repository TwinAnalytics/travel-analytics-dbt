/*
  dim_employees
  -------------
  Employee dimension table (SCD Type 0 — history not tracked, snapshot at load time).
  Grain: one row per employee.

  Adds:
    - tenure_days: days since employee start date (as of model run date)
*/

with

employees as (
    select * from {{ ref('stg_employees') }}
),

companies as (
    select
        company_id,
        company_name,
        size_tier,
        industry
    from {{ ref('stg_companies') }}
),

final as (
    select
        -- primary key
        e.employee_id,

        -- foreign key
        e.company_id,

        -- company context (denormalised)
        c.company_name,
        c.size_tier,
        c.industry,

        -- employee attributes
        e.department,
        e.seniority,
        e.country,
        e.start_date,
        e.is_active,

        -- derived measures
        datediff('day', e.start_date, current_date) as tenure_days

    from employees e
    left join companies c
        on e.company_id = c.company_id
)

select * from final

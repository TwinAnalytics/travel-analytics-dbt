/*
  stg_expenses
  ------------
  Thin staging layer for raw expense submissions.
  Responsibilities:
    - Rename columns to follow project conventions
    - Cast to correct data types
    - No business logic applied here
*/

with

source as (
    select * from {{ source('raw', 'raw_expenses') }}
),

renamed as (
    select
        -- identifiers
        cast(expense_id  as varchar)  as expense_id,
        cast(employee_id as varchar)  as employee_id,
        cast(company_id  as varchar)  as company_id,

        -- dates
        cast(expense_date as date)    as expense_date,

        -- expense details
        cast(category     as varchar) as category,
        cast(amount_cents as integer) as amount_cents,
        cast(currency     as varchar) as currency,
        cast(status       as varchar) as status,

        -- boolean flags
        cast(receipt_attached  as boolean) as receipt_attached,
        cast(policy_compliant  as boolean) as policy_compliant

    from source
    where expense_id is not null
)

select * from renamed

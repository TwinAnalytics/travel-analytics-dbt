/*
  stg_employees
  -------------
  Thin staging layer for employee master data.
  Responsibilities:
    - Cast to correct data types
    - No business logic applied here
*/

with

source as (
    select * from {{ source('raw', 'raw_employees') }}
),

renamed as (
    select
        -- identifiers
        cast(employee_id as varchar) as employee_id,
        cast(company_id  as varchar) as company_id,

        -- employee attributes
        cast(department  as varchar) as department,
        cast(seniority   as varchar) as seniority,
        cast(country     as varchar) as country,

        -- dates
        cast(start_date  as date)    as start_date,

        -- boolean flags
        cast(is_active   as boolean) as is_active

    from source
    where employee_id is not null
)

select * from renamed

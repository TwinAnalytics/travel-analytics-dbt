/*
  stg_companies
  -------------
  Thin staging layer for company / customer master data.
  Responsibilities:
    - Cast to correct data types
    - No business logic applied here
*/

with

source as (
    select * from {{ source('raw', 'raw_companies') }}
),

renamed as (
    select
        -- identifiers
        cast(company_id    as varchar) as company_id,

        -- company attributes
        cast(company_name  as varchar) as company_name,
        cast(industry      as varchar) as industry,
        cast(size_tier     as varchar) as size_tier,
        cast(country       as varchar) as country,

        -- dates
        cast(contract_start as date)   as contract_start,

        -- financials
        cast(monthly_mrr_eur as numeric(12,2)) as monthly_mrr_eur

    from source
    where company_id is not null
)

select * from renamed

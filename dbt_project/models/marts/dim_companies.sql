/*
  dim_companies
  -------------
  Company / customer dimension table.
  Grain: one row per company.

  Adds:
    - mrr_tier: bucketed MRR classification for reporting
    - contract_age_days: days since contract start
*/

with

companies as (
    select * from {{ ref('stg_companies') }}
),

final as (
    select
        -- primary key
        company_id,

        -- company attributes
        company_name,
        industry,
        size_tier,
        country,
        contract_start,
        monthly_mrr_eur,

        -- derived measures
        datediff('day', contract_start, current_date) as contract_age_days,

        -- MRR tier bucketing
        case
            when monthly_mrr_eur < 1000   then 'Low (<1k)'
            when monthly_mrr_eur < 5000   then 'Mid (1k-5k)'
            when monthly_mrr_eur < 20000  then 'High (5k-20k)'
            else 'Premium (20k+)'
        end                                            as mrr_tier

    from companies
)

select * from final

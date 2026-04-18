/*
  fct_expenses
  ------------
  Fact table for employee expense submissions.
  Grain: one row per expense.
*/

with

enriched as (
    select * from {{ ref('int_expenses_enriched') }}
),

final as (
    select
        -- surrogate / natural keys
        expense_id,
        employee_id,
        company_id,

        -- employee & company attributes (denormalised)
        department,
        seniority,
        employee_country,
        company_name,
        industry,
        size_tier,

        -- expense dates & period
        expense_date,
        expense_month,

        -- expense details
        category,
        amount_cents,
        currency,
        status,

        -- measures
        cost_eur,

        -- flags
        receipt_attached,
        policy_compliant                      as is_policy_compliant

    from enriched
)

select * from final

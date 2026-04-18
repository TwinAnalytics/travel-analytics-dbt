/*
  fct_bookings
  ------------
  Fact table for travel bookings.
  Grain: one row per booking.

  Adds:
    - is_policy_compliant (aliased for clarity)
    - advance_booking_tier (last_minute / standard / advance)
*/

with

enriched as (
    select * from {{ ref('int_bookings_enriched') }}
),

final as (
    select
        -- surrogate / natural keys
        booking_id,
        employee_id,
        company_id,

        -- employee & company attributes (denormalised for query performance)
        department,
        seniority,
        employee_country,
        company_name,
        industry,
        size_tier,

        -- booking dates & period
        booking_date,
        travel_date,
        booking_month,

        -- location
        destination_city,
        destination_country,

        -- booking details
        trip_type,
        amount_cents,
        currency,
        status,
        advance_days,

        -- measures
        cost_eur,

        -- derived flags
        is_cancelled,
        policy_compliant                      as is_policy_compliant,

        -- advance booking tier classification
        case
            when advance_days < 3  then 'last_minute'
            when advance_days <= 14 then 'standard'
            else 'advance'
        end                                   as advance_booking_tier

    from enriched
)

select * from final

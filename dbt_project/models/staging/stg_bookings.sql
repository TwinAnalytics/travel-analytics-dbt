/*
  stg_bookings
  ------------
  Thin staging layer for raw bookings data.
  Responsibilities:
    - Rename columns to follow project conventions
    - Cast to correct data types
    - Filter out records with null primary keys
    - No business logic applied here
*/

with

source as (
    select * from {{ source('raw', 'raw_bookings') }}
),

renamed as (
    select
        -- identifiers
        cast(booking_id       as varchar)   as booking_id,
        cast(employee_id      as varchar)   as employee_id,
        cast(company_id       as varchar)   as company_id,

        -- dates
        cast(booking_date     as date)      as booking_date,
        cast(travel_date      as date)      as travel_date,

        -- location
        cast(destination_city    as varchar) as destination_city,
        cast(destination_country as varchar) as destination_country,

        -- booking details
        cast(trip_type        as varchar)   as trip_type,
        cast(amount_cents     as integer)   as amount_cents,
        cast(currency         as varchar)   as currency,
        cast(status           as varchar)   as status,
        cast(advance_days     as integer)   as advance_days,

        -- boolean flags (DuckDB stores as boolean)
        cast(policy_compliant as boolean)   as policy_compliant

    from source
    where booking_id is not null
)

select * from renamed

/*
  assert_spend_positive
  ---------------------
  Singular test: asserts that every booking in stg_bookings has a positive
  amount_cents value. Any row returned by this query is considered a test failure.

  Business rationale: a booking with zero or negative spend indicates a data
  quality issue upstream (refunds are tracked separately via status = 'cancelled',
  not as negative amounts).
*/

select
    booking_id,
    amount_cents
from {{ ref('stg_bookings') }}
where amount_cents <= 0

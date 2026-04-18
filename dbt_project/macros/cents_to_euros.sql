/*
  cents_to_euros(column)
  ----------------------
  Converts an integer amount in cents to EUR by dividing by 100
  and rounding to 2 decimal places.

  Usage:
    {{ cents_to_euros('amount_cents') }}

  Example:
    SELECT {{ cents_to_euros('amount_cents') }} AS amount_eur
    -- 12345 -> 123.45
*/

{% macro cents_to_euros(column) %}
    round(cast({{ column }} as numeric) / 100.0, 2)
{% endmacro %}

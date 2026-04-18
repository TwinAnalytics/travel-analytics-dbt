/*
  load_raw_csvs()
  ---------------
  On-run-start macro that loads the synthetic CSV files into DuckDB
  as raw source tables. This simulates the ingestion layer (Fivetran/Airbyte
  in production) that would land data into the raw schema in Snowflake.

  The data directory is passed via the 'data_dir' variable:
    dbt run --vars '{"data_dir": "/absolute/path/to/data"}'

  Or set the DATA_DIR environment variable in profiles.yml.

  Tables created:
    - main.raw_bookings
    - main.raw_expenses
    - main.raw_employees
    - main.raw_companies
*/

{% macro load_raw_csvs() %}

  {#
    Resolve the data directory from variable.
    Pass via: --vars '{"data_dir": "/path/to/data"}'
  #}
  {% set data_dir = var('data_dir', '') %}
  {% if data_dir == '' %}
    {% set data_dir = env_var('PERK_DATA_DIR', '../data') %}
  {% endif %}

  {% set sql %}
    CREATE OR REPLACE TABLE main.raw_companies AS
      SELECT * FROM read_csv_auto('{{ data_dir }}/companies.csv', header=true);

    CREATE OR REPLACE TABLE main.raw_employees AS
      SELECT * FROM read_csv_auto('{{ data_dir }}/employees.csv', header=true);

    CREATE OR REPLACE TABLE main.raw_bookings AS
      SELECT * FROM read_csv_auto('{{ data_dir }}/bookings.csv', header=true);

    CREATE OR REPLACE TABLE main.raw_expenses AS
      SELECT * FROM read_csv_auto('{{ data_dir }}/expenses.csv', header=true);
  {% endset %}

  {{ return(sql) }}

{% endmacro %}

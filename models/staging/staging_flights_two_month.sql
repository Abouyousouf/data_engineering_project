{{ config(materialized='view') }}

WITH flights_two_month AS (
    SELECT * 
    FROM {{source('flights_data', 'flights_all_jan_feb_2024')}}
    WHERE DATE_PART('month', flight_date) = 2 
)
SELECT * FROM flights_two_month
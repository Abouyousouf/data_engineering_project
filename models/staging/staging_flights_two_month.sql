{{ config(materialized='view') }}

WITH flights_two_month AS (
    SELECT * 
    FROM {{source('flights_data', 'flights_selected')}}
    WHERE DATE_PART('month', flight_date) = 2 
)
SELECT * FROM flights_two_month
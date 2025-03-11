{{ config(materialized='view') }}

WITH flights_january AS (
    SELECT * 
    FROM {{source('flights_data', 'flights')}}
    WHERE DATE_PART('month', flight_date) = 1 
)
SELECT * FROM flights_january
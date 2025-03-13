WITH airports_regions_join AS (
    SELECT * 
    FROM {{source('flights_data', 'airports_selected')}}
    LEFT JOIN {{source('flights_data', 'regions')}}
    USING (country)
)
SELECT * FROM airports_regions_join
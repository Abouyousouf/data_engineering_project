WITH daily_raw AS (
    SELECT
        airport_code,
        station_id,
        JSON_ARRAY_ELEMENTS((extracted_data::jsonb) -> 'data') AS json_data
    FROM {{source('weather_data', 'weather_daily_all')}}
),
daily_flattened AS (
    SELECT  
        airport_code,
        station_id,
        (json_data->>'date')::DATE AS date,
        COALESCE((json_data->>'tavg')::NUMERIC, 0) AS avg_temp_c,
        COALESCE((json_data->>'tmin')::NUMERIC, 0) AS min_temp_c,
        COALESCE((json_data->>'tmax')::NUMERIC, 0) AS max_temp_c,
        COALESCE((json_data->>'prcp')::NUMERIC, 0) AS precipitation_mm,
        COALESCE((json_data->>'snow')::INTEGER, 0) AS max_snow_mm,
        COALESCE((json_data->>'wdir')::INTEGER, 0) AS avg_wind_direction,
        COALESCE((json_data->>'wspd')::NUMERIC, 0) AS avg_wind_speed_kmh,
        COALESCE((json_data->>'wpgt')::NUMERIC, 0) AS wind_peakgust_kmh,
        COALESCE((json_data->>'pres')::NUMERIC, 0) AS avg_pressure_hpa,
        COALESCE((json_data->>'tsun')::INTEGER, 0) AS sun_minutes
    FROM daily_raw
)
SELECT * 
FROM daily_flattened;
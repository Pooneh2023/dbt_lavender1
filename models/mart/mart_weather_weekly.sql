WITH daily_data AS (
        SELECT * 
        FROM {{ref('prep_weather_daily')}}
),
weekly_aggregation AS (
        SELECT airport_code, station_id, date_year, cw
            ,AVG(avg_temp_c)::NUMERIC(4,2) AS avg_temp_c
            ,MIN(min_temp_c)::NUMERIC(4,2) AS min_temp_c
            ,MAX(max_temp_c)::NUMERIC(4,2) AS max_temp_c
            ,SUM(precipitation_mm) AS total_prec_mm
            ,SUM(max_snow_mm) AS total_max_snow_mm
            ,AVG(avg_wind_direction)::NUMERIC(5,2) AS avg_wind_direction
            ,AVG(avg_wind_speed_kmh)::NUMERIC(5,2) AS avg_wind_speed_kmh
            ,MAX(wind_peakgust_kmh)::NUMERIC(5,2) AS wind_peakgust_kmh
            ,AVG(avg_pressure_hpa)::NUMERIC(6,2) AS avg_pressure_hpa
            ,SUM(sun_minutes) AS total_sun_minutes
            ,MODE() WITHIN GROUP (ORDER BY date_month) AS month
            ,MODE() WITHIN GROUP (ORDER BY month_name) AS month_name
            ,MODE() WITHIN GROUP (ORDER BY season) AS season
        FROM daily_data
        GROUP BY airport_code, station_id, date_year, cw
)
SELECT * FROM weekly_aggregation
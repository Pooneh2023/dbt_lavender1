    WITH hourly_data AS (
        SELECT * 
        FROM {{ref('staging_weather_hourly')}}
    ),
    add_features AS (
        SELECT *
    		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
    		, ... AS time                           -- only time (hours:minutes:seconds) as TIME data type
            , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
            , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
            , ... AS weekday        -- weekday name as TEXT        
            , DATE_PART('day', timestamp) AS date_day
    		, ... AS date_month
    		, ... AS date_year
    		, ... AS cw
        FROM hourly_data
    ),
    add_more_features AS (
        SELECT *
    		,(CASE 
    			WHEN time BETWEEN ... AND ... THEN 'night'
    			WHEN ... THEN 'day'
    			WHEN ... THEN 'evening'
    		END) AS day_part
        FROM add_features
    )
    
    SELECT *
    FROM add_more_features
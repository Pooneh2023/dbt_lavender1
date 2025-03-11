 WITH daily_data AS (
        SELECT * 
        FROM {{ref('staging_weather_daily')}}
    ),
    add_features AS (
        SELECT *
    		, ... AS date_day 		-- number of the day of month
    		, ... AS date_month 	-- number of the month of year
    		, ... AS date_year 		-- number of year
    		, ... AS cw 			-- number of the week of year
    		, ... AS month_name 	-- name of the month
    		, ... AS weekday 		-- name of the weekday
        FROM daily_data 
    ),
    add_more_features AS (
        SELECT *
    		, (CASE 
    			WHEN month_name in ... THEN 'winter'
    			WHEN ... THEN 'spring'
                WHEN ... THEN 'summer'
                WHEN ... THEN 'autumn'
    		END) AS season
        FROM add_features
    )
    SELECT *
    FROM add_more_features
    ORDER BY date
WITH flights_stats AS (
	SELECT -- TO_CHAR(flight_date, 'YYYY-MM') AS flight_month, --for the alternative GROUPBY also by month (see below)
		   origin
		   ,dest
        --    ,origin || ' - ' || dest AS route -- optional (challenge: keep only 'route' in the final query output, w/o 'origin' and 'dest')
		   ,COUNT(flight_number) AS n_flights
		   ,COUNT(DISTINCT tail_number) AS nunique_tails
		   ,COUNT(DISTINCT airline) AS nunique_airlines
		   ,AVG(actual_elapsed_time)::INTEGER * ('1 second'::INTERVAL) AS avg_actual_elapsed_time 
        --    ,(AVG(actual_elapsed_time)*60)::INTEGER * ('1 second'::INTERVAL) AS avg_actual_elapsed_time -- without rounding the seconds
		--    ,STDDEV(actual_elapsed_time)::INTEGER * ('1 second'::INTERVAL) AS sd_actual_elapsed_time 
		   ,AVG(arr_delay)::INTEGER * ('1 second'::INTERVAL) AS avg_arr_delay
		   ,MIN(arr_delay_interval) AS min_arr_delay
		   ,MAX(arr_delay_interval) AS max_arr_delay
		   ,SUM(cancelled) AS total_cancelled
		   ,SUM(diverted) AS total_diverted
	FROM {{ref('prep_flights')}}
    GROUP BY (origin, dest) -- over all time
	-- GROUP BY (flight_month, origin, dest) -- alternative GROUPBY also by month
),
add_names AS (
	SELECT o.city AS origin_city
			,d.city AS dest_city
			,o.name AS origin_name
			,d.name AS dest_name
			,f.*
	FROM flights_stats f
	LEFT JOIN {{ref('prep_airports')}} o
		ON origin=o.faa
	LEFT JOIN {{ref('prep_airports')}} d
		ON dest=d.faa
)
SELECT *
FROM add_names
ORDER BY (origin, dest) DESC
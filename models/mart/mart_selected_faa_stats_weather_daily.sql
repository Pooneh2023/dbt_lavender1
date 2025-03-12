WITH departures AS ( 
	SELECT flight_date 
			,origin AS faa
			,COUNT(DISTINCT dest) AS nunique_to
			,COUNT(sched_dep_time) AS dep_planned
			,SUM(cancelled) AS dep_cancelled
			,SUM(diverted) AS dep_diverted
			,COUNT(arr_time) AS dep_n_flights
--			,COUNT(DISTINCT tail_number) AS dep_nunique_tails -- BONUS TASK
--			,COUNT(DISTINCT airline) AS dep_nunique_airlines -- BONUS TASK
	FROM {{ref('prep_flights')}}
	WHERE origin IN (SELECT DISTINCT airport_code FROM {{ref('prep_weather_daily')}})
	GROUP BY origin, flight_date 
	ORDER BY origin, flight_date
),
arrivals AS (
	SELECT flight_date
			,dest AS faa
			,COUNT(DISTINCT origin) AS nunique_from
			,COUNT(sched_dep_time) AS arr_planned
			,SUM(cancelled) AS arr_cancelled
			,SUM(diverted) AS arr_diverted
			,COUNT(arr_time) AS arr_n_flights
--			,COUNT(DISTINCT tail_number) AS arr_nunique_tails -- BONUS TASK
--			,COUNT(DISTINCT airline) AS arr_nunique_airlines -- BONUS TASK
	FROM {{ref('prep_flights')}}
	WHERE dest IN (SELECT DISTINCT airport_code FROM {{ref('prep_weather_daily')}})
	GROUP BY dest, flight_date
	ORDER BY dest, flight_date
),
total_stats AS (
	SELECT flight_date
			,faa
			,nunique_to
			,nunique_from
	--		,(nunique_to + nunique_from)::NUMERIC/2 AS n_connections -- fractions would indicate that for the number of connections to!=from --BONUS task
			,dep_planned + arr_planned AS total_planned
			,dep_cancelled + arr_cancelled AS total_cancelled
			,dep_diverted + arr_diverted AS total_diverted
	--		,((dep_cancelled + arr_cancelled + dep_diverted + arr_diverted)::NUMERIC/(dep_planned + arr_planned)::NUMERIC)*100 AS percent_change -- BONUS TASK
			,dep_n_flights + arr_n_flights AS total_flights
	--		,(dep_nunique_tails + arr_nunique_tails)::NUMERIC/2 AS nunique_tails -- fractions would indicate that for the number of tails to!=from -- BONUS TASK
	--		,(dep_nunique_airlines + arr_nunique_airlines)::NUMERIC/2 AS nunique_airlines -- fractions would indicate that for the number of airlines to!=from -- BONUS TASK
	FROM departures
	JOIN arrivals
	USING (flight_date, faa)
)
SELECT t.* 
		,w.min_temp_c
		,w.max_temp_c
		,w.precipitation_mm
		,w.max_snow_mm
		,w.avg_wind_direction
		,w.avg_wind_speed_kmh
		,w.wind_peakgust_kmh
FROM total_stats t
LEFT JOIN {{ref('prep_weather_daily')}} w
ON faa = airport_code AND flight_date = date
ORDER BY total_diverted DESC
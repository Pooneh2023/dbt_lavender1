WITH departures AS ( 
	SELECT origin AS faa
			,COUNT(DISTINCT dest) AS nunique_to
			,COUNT(sched_dep_time) AS dep_planned
			,SUM(cancelled) AS dep_cancelled
			,SUM(diverted) AS dep_diverted
			,COUNT(arr_time) AS dep_n_flights
			-- ,COUNT(DISTINCT tail_number) AS dep_nunique_tails -- BONUS TASK
			-- ,COUNT(DISTINCT airline) AS dep_nunique_airlines -- BONUS TASK
	FROM {{ref('prep_flights')}} 
	GROUP BY origin
	ORDER BY origin
),
arrivals AS (
	SELECT dest AS faa
			,COUNT(DISTINCT origin) AS nunique_from
			,COUNT(sched_dep_time) AS arr_planned
			,SUM(cancelled) AS arr_cancelled
			,SUM(diverted) AS arr_diverted
			,COUNT(arr_time) AS arr_n_flights
			-- ,COUNT(DISTINCT tail_number) AS arr_nunique_tails -- BONUS TASK
			-- ,COUNT(DISTINCT airline) AS arr_nunique_airlines -- BONUS TASK
	FROM {{ref('prep_flights')}} 
	GROUP BY dest
	ORDER BY dest
),
total_stats AS (
	SELECT faa
			,nunique_to
			,nunique_from
	--		,(nunique_to + nunique_from)::NUMERIC/2 AS n_connections -- fractions would indicate that for the number of connections to!=from 
			,dep_planned + arr_planned AS total_planned
			,dep_cancelled + arr_cancelled AS total_cancelled
			,dep_diverted + arr_diverted AS total_diverted
	--		,((dep_cancelled + arr_cancelled + dep_diverted + arr_diverted)::NUMERIC/(dep_planned + arr_planned)::NUMERIC)*100 AS percent_change -- BONUS TASK
			,dep_n_flights + arr_n_flights AS total_flights
	--		,(dep_nunique_tails + arr_nunique_tails)::NUMERIC/2 AS nunique_tails -- fractions would indicate that for the number of tails to!=from -- BONUS TASK
	--		,(dep_nunique_airlines + arr_nunique_airlines)::NUMERIC/2 AS nunique_airlines -- fractions would indicate that for the number of airlines to!=from -- BONUS TASK
	FROM departures
	JOIN arrivals
	USING (faa)
)
SELECT a.city
		,a.country
		,a.name
		, t.* 
FROM total_stats t
LEFT JOIN {{ref('prep_airports')}} a
USING (faa)
ORDER BY total_diverted DESC
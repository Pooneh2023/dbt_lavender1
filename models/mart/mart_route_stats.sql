WITH flight_route_stats AS (
SELECT origin,
		dest,
		count(flight_number) AS n_flights,
		count(DISTINCT tail_number) AS nUNIQUE_tails,
		count(DISTINCT airline) AS nunique_airlines,
		avg(actual_elapsed_time_interval) AS avg_actual_elapsed_time
		,avg(arr_delay_interval) AS avg_arr_delay
		,avg(dep_delay_interval) AS avg_dep_delay
		,max(arr_delay_interval) AS max_arr_delay
		,min(arr_delay_interval) AS min_arr_delay
		,sum(cancelled) AS total_canceled
		,sum(diverted) AS total_diverted
FROM {{ref('PREP_FLIGHTS')}}
GROUP BY (origin,dest)
)
SELECT o.city AS origin_city
		,d.city AS dest_city
		,o.name AS origin_name
		,d.name AS dest_name
	   ,o.country AS origin_country
	   ,d.country AS dest_country
	   ,*
FROM flight_route_stats f
LEFT JOIN {{ref('PREP_airports')}} o
ON f.origin=o.faa
LEFT JOIN {{ref('PREp_airports')}} d
ON f.dest=d.faa;

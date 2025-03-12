WITH flight_route_stats AS (
        SELECT 
            origin 
            ,dest
            ,COUNT(flight_number) AS n_flights
            ,COUNT(DISTINCT tail_number) AS nunique_tails
            ,COUNT(DISTINCT airline) AS nunique_airlines
            ,AVG(actual_elapsed_time_interval) AS avg_actual_elapsed_time
            ,AVG(arr_delay_interval) AS avg_arr_delay
            ,AVG(dep_delay_interval) AS avg_dep_delay
            ,MAX(arr_delay_interval) AS max_arr_delay
            ,MIN(arr_delay_interval) AS min_arr_delay
            ,SUM(cancelled) AS total_canceled
            ,SUM(diverted) AS total_diverted
        FROM {{ref('prep_flights')}}
        GROUP BY (origin, dest) 
)
SELECT o.city AS origin_city, 
        d.city AS dest_city,
        o.name AS origin_name,
        d.name AS dest_name,
        o.country AS origin_country, 
        d.country AS dest_country,
        f.*
FROM flight_route_stats f
LEFT JOIN {{ref('prep_airports')}} o
    ON f.origin=o.faa
LEFT JOIN {{ref('prep_airports')}} d
    ON f.dest=d.faa
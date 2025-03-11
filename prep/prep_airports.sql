WITH airports_reorder AS (
        SELECT faa
        	   ,...
        	   ...
        FROM {{ref('staging_airports')}}
    )
SELECT * FROM airports_reorder
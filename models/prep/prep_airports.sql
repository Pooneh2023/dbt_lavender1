WITH airports_reorder AS (
        SELECT faa
        	   ,name,lat,lon,alt,
        	
        FROM {{ref('staging_airports')}}
    )
SELECT * FROM airports_reorder
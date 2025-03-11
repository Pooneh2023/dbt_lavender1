WITH airports_reorder AS (
        SELECT faa
        	   ,name
               ,lat
               ,lon
               ,alt
               ,tz
               ,dst
               ,city
               ,country
        	
        FROM {{ref('staging_airports')}}
    )
SELECT * FROM airports_reorder
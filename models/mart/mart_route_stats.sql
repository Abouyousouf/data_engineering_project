WITH flight_route_stats AS(

		SELECT COUNT(flight_number)AS COUNT_flights
			,origin
			,dest
			,COUNT (DISTINCT tail_number) AS COUNT_UNIQUE_tail_number
			,COUNT(DISTINCT airline) AS COUNT_UNIQUE_airline
			,AVG(actual_elapsed_time_interval) AS AVG_elapsed_time_interval
			,AVG(arr_delay_interval) AS AVG_arr_delay_interval
			,MAX(arr_delay_interval) AS MAX_arr_delay
			,MIN(arr_delay_interval) AS MIN_arr_delay
			,SUM(cancelled) AS SUM_cancelled_flights
			,SUM(diverted) AS SUM_diverted
		FROM {{ref('prep_flights')}}
		GROUP BY origin, dest
		ORDER BY COUNT_flights DESC
)
SELECT o.city AS origin_city
		,d.city AS dest_city
		,o.name AS origin_name
		,d.name AS dest_name
		,o.country AS origin_country
		,d.country AS dest_country
		,f.*
FROM flight_route_stats f
LEFT JOIN {{ref('prep_airports')}} o
	ON f.origin=o.faa
LEFT JOIN {{ref('prep_airports')}} d
	ON f.dest=d.faa
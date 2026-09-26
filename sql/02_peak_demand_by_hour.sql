-- Peak demand hours by day of week (1 = Monday).
SELECT day_of_week(pickup_datetime) AS dow,
       hour(pickup_datetime)        AS pickup_hour,
       COUNT(*)                     AS trips
FROM nyc_taxi_processed.trips_processed
WHERE year = 2024 AND month BETWEEN 1 AND 3
GROUP BY 1, 2
ORDER BY dow, pickup_hour;

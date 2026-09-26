-- Partition pruning check: only year=2024/month=1 is scanned.
SELECT service,
       COUNT(*)                     AS trips,
       ROUND(AVG(fare_amount), 2)   AS avg_fare,
       ROUND(AVG(trip_distance), 2) AS avg_miles
FROM nyc_taxi_processed.trips_processed
WHERE year = 2024 AND month = 1
GROUP BY service
ORDER BY trips DESC;

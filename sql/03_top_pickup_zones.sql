-- Busiest pickup zones, with names from the TLC zone lookup.
SELECT z.borough,
       z.zone,
       COUNT(*) AS trips
FROM nyc_taxi_processed.trips_processed t
JOIN nyc_taxi_processed.taxi_zones z
  ON t.pulocationid = CAST(z.locationid AS integer)
WHERE t.year = 2024 AND t.month = 1
GROUP BY 1, 2
ORDER BY trips DESC
LIMIT 20;

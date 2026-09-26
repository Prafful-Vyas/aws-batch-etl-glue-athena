-- Average fare and tip rate by pickup borough and month.
-- Tip rate uses card payments only (payment_type = 1): cash tips aren't recorded.
SELECT t.year,
       t.month,
       z.borough,
       COUNT(*)                                          AS trips,
       ROUND(AVG(t.fare_amount), 2)                      AS avg_fare,
       ROUND(AVG(t.tip_amount / NULLIF(t.fare_amount, 0))
             FILTER (WHERE t.payment_type = 1) * 100, 1) AS avg_tip_pct_card
FROM nyc_taxi_processed.trips_processed t
JOIN nyc_taxi_processed.taxi_zones z
  ON t.pulocationid = CAST(z.locationid AS integer)
WHERE t.year = 2024
GROUP BY 1, 2, 3
ORDER BY 1, 2, trips DESC;

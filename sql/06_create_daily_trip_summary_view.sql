-- Reusable daily rollup for dashboards. A view (not CTAS) because the
-- workgroup enforces its result location, which rules out CTAS external_location.
CREATE OR REPLACE VIEW nyc_taxi_processed.daily_trip_summary AS
SELECT service,
       year,
       month,
       date(pickup_datetime)            AS trip_date,
       COUNT(*)                         AS trips,
       SUM(total_amount)                AS revenue,
       AVG(trip_distance)               AS avg_miles,
       AVG(trip_duration_min)           AS avg_duration_min
FROM nyc_taxi_processed.trips_processed
GROUP BY 1, 2, 3, 4;

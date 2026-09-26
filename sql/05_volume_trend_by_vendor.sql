-- Monthly trip volume trend by service and vendor.
SELECT service,
       vendorid,
       year,
       month,
       COUNT(*) AS trips
FROM nyc_taxi_processed.trips_processed
WHERE year >= 2024
GROUP BY 1, 2, 3, 4
ORDER BY service, vendorid, year, month;

-- Leaks by location (possible low-pressure events)
SELECT location, COUNT(*) AS leak_count
FROM sensor_data
WHERE is_leak = 1
GROUP BY location;

-- Overall average pressure across all readings
SELECT AVG(pressure) AS avg_pressure_all_readings
FROM sensor_data;

-- Ops view: ranked locations by average pressure (from pre-aggregated load table)
SELECT location, avg_pressure, avg_flow
FROM sensor_agg_by_location
ORDER BY avg_pressure ASC;

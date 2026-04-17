-- Analytics on gold tables only

SELECT location, leak_count
FROM gold_leaks_by_location
ORDER BY location;

SELECT SUM(avg_pressure * reading_count) * 1.0 / SUM(reading_count) AS avg_pressure_all_readings
FROM gold_sensor_agg_by_location;

SELECT location, avg_pressure, avg_flow, reading_count
FROM gold_sensor_agg_by_location
ORDER BY avg_pressure ASC;

SELECT reading_date, location, avg_pressure, avg_flow
FROM gold_daily_pressure_summary
ORDER BY reading_date, location;

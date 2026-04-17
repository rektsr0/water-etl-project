-- Gold-layer analytics (curated tables; consumers do not query raw bronze directly)

-- Leak-flagged reading counts by location (pre-aggregated gold table)
SELECT location, leak_count
FROM gold_leaks_by_location
ORDER BY location;

-- Overall average pressure across all silver readings (weighted by location reading counts in gold agg)
SELECT SUM(avg_pressure * reading_count) * 1.0 / SUM(reading_count) AS avg_pressure_all_readings
FROM gold_sensor_agg_by_location;

-- Ranked sites by average pressure / flow (gold dimensional summary)
SELECT location, avg_pressure, avg_flow, reading_count
FROM gold_sensor_agg_by_location
ORDER BY avg_pressure ASC;

-- Daily operational view (gold time-series summary)
SELECT reading_date, location, avg_pressure, avg_flow
FROM gold_daily_pressure_summary
ORDER BY reading_date, location;

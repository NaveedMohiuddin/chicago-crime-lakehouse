-- =====================================================
-- ChiCrime Insight Lakehouse - Athena Query Library
-- =====================================================

-- Query 1: Overall Crime Summary
-- Purpose: High-level metrics for entire dataset
-- Runtime: ~3 seconds
-- =====================================================
SELECT 
    COUNT(*) AS total_crimes,
    COUNT(DISTINCT crime_id) as unique_crimes,
    COUNT(DISTINCT dt) as days_of_data,
    MIN(date_ts) as earliest_crime,
    MAX(date_ts) as latest_crime,
    SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) as total_arrests,
    ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) as arrest_rate_pct
FROM crimes_iceberg;


-- Query 2: Top 10 Crime Types
-- Purpose: Identify most common crimes and clearance rates
-- Runtime: ~2 seconds
-- =====================================================
SELECT 
    primary_type,
    COUNT(*) as crime_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total,
    SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) as arrests,
    ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) as arrest_rate_pct
FROM crimes_iceberg
GROUP BY primary_type
ORDER BY crime_count DESC
LIMIT 10;


-- Query 3: Crime Patterns by Time of Day
-- Purpose: Show when crimes occur for patrol scheduling
-- Runtime: ~2 seconds
-- =====================================================
SELECT 
    time_of_day,
    COUNT(*) as crime_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total,
    SUM(CASE WHEN is_violent = true THEN 1 ELSE 0 END) as violent_crimes,
    SUM(CASE WHEN is_property = true THEN 1 ELSE 0 END) as property_crimes
FROM crimes_iceberg
GROUP BY time_of_day
ORDER BY 
    CASE time_of_day
        WHEN 'Morning' THEN 1
        WHEN 'Afternoon' THEN 2
        WHEN 'Evening' THEN 3
        WHEN 'Night' THEN 4
    END;


-- Query 4: Hourly Crime Heatmap
-- Purpose: Exact hour patterns for resource allocation
-- Runtime: ~2 seconds
-- =====================================================
SELECT 
    hour,
    COUNT(*) as crime_count,
    SUM(CASE WHEN is_violent = true THEN 1 ELSE 0 END) as violent,
    SUM(CASE WHEN is_property = true THEN 1 ELSE 0 END) as property,
    SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) as arrests,
    ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) as arrest_rate_pct
FROM crimes_iceberg
GROUP BY hour
ORDER BY hour;


-- Query 5: Crime by District
-- Purpose: Identify hotspot districts needing resources
-- Runtime: ~3 seconds
-- =====================================================
SELECT 
    district,
    COUNT(*) as total_crimes,
    SUM(CASE WHEN is_violent = true THEN 1 ELSE 0 END) as violent_crimes,
    SUM(CASE WHEN is_property = true THEN 1 ELSE 0 END) as property_crimes,
    SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) as arrests,
    ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) as arrest_rate_pct
FROM crimes_iceberg
WHERE district IS NOT NULL
GROUP BY district
ORDER BY total_crimes DESC
LIMIT 15;


-- Query 6: Day of Week Patterns
-- Purpose: Weekend vs. weekday crime trends
-- Runtime: ~2 seconds
-- =====================================================
SELECT 
    CASE day_of_week
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_name,
    COUNT(*) as crime_count,
    SUM(CASE WHEN is_violent = true THEN 1 ELSE 0 END) as violent_crimes,
    ROUND(AVG(CASE WHEN arrest = true THEN 1.0 ELSE 0.0 END) * 100, 2) as arrest_rate_pct
FROM crimes_iceberg
GROUP BY day_of_week
ORDER BY day_of_week;


-- Query 7: Location Type Analysis
-- Purpose: Where crimes occur (streets, residences, etc.)
-- Runtime: ~3 seconds
-- =====================================================
SELECT 
    location_description,
    COUNT(*) as crime_count,
    SUM(CASE WHEN is_violent = true THEN 1 ELSE 0 END) as violent_count,
    SUM(CASE WHEN is_property = true THEN 1 ELSE 0 END) as property_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM crimes_iceberg
WHERE location_description IS NOT NULL
GROUP BY location_description
ORDER BY crime_count DESC
LIMIT 15;


-- Query 8: Domestic Violence Analysis
-- Purpose: Domestic incident patterns
-- Runtime: ~2 seconds
-- =====================================================
SELECT 
    primary_type,
    COUNT(*) as total_incidents,
    SUM(CASE WHEN domestic = true THEN 1 ELSE 0 END) as domestic_incidents,
    ROUND(100.0 * SUM(CASE WHEN domestic = true THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_domestic,
    SUM(CASE WHEN domestic = true AND arrest = true THEN 1 ELSE 0 END) as domestic_arrests,
    ROUND(100.0 * SUM(CASE WHEN domestic = true AND arrest = true THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN domestic = true THEN 1 ELSE 0 END), 0), 2) as domestic_arrest_rate
FROM crimes_iceberg
WHERE primary_type IN ('BATTERY', 'ASSAULT', 'CRIMINAL SEXUAL ASSAULT', 'CRIM SEXUAL ASSAULT')
GROUP BY primary_type
ORDER BY total_incidents DESC;


-- Query 9: Monthly Trend Analysis
-- Purpose: Seasonal crime patterns
-- Runtime: ~3 seconds
-- =====================================================
SELECT 
    year,
    month,
    COUNT(*) as crime_count,
    SUM(CASE WHEN is_violent = true THEN 1 ELSE 0 END) as violent_crimes,
    SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) as arrests,
    ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) as arrest_rate_pct
FROM crimes_iceberg
GROUP BY year, month
ORDER BY year DESC, month DESC;


-- Query 10: High-Risk Blocks (Repeat Offense Locations)
-- Purpose: Identify addresses needing intervention
-- Runtime: ~4 seconds
-- =====================================================
SELECT 
    block,
    COUNT(*) as crime_count,
    COUNT(DISTINCT primary_type) as crime_types,
    SUM(CASE WHEN is_violent = true THEN 1 ELSE 0 END) as violent_crimes,
    SUM(CASE WHEN is_property = true THEN 1 ELSE 0 END) as property_crimes,
    MAX(date_ts) as most_recent_crime,
    ROUND(AVG(CASE WHEN arrest = true THEN 1.0 ELSE 0.0 END) * 100, 2) as avg_arrest_rate
FROM crimes_iceberg
WHERE block IS NOT NULL
GROUP BY block
HAVING COUNT(*) >= 5
ORDER BY crime_count DESC
LIMIT 20;
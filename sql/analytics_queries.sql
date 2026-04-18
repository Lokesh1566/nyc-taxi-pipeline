-- Business queries used by the dashboard + ad-hoc analysis.
-- Organized by dashboard widget so it's easy to trace back.

-- ====================================================================
-- Widget: Trip volume by hour (last 24h rolling)
-- ====================================================================
SELECT
    PICKUP_HOUR_TS,
    SUM(TRIP_COUNT) AS total_trips,
    SUM(TOTAL_REVENUE) AS total_revenue
FROM FCT_TRIPS_HOURLY
WHERE PICKUP_HOUR_TS >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY PICKUP_HOUR_TS
ORDER BY PICKUP_HOUR_TS;


-- ====================================================================
-- Widget: Revenue by pickup borough
-- ====================================================================
SELECT
    PICKUP_BOROUGH,
    SUM(TOTAL_REVENUE) AS revenue,
    SUM(TRIP_COUNT) AS trips,
    SUM(TOTAL_REVENUE) / NULLIF(SUM(TRIP_COUNT), 0) AS avg_revenue_per_trip
FROM FCT_TRIPS_HOURLY
WHERE PICKUP_HOUR_TS >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY PICKUP_BOROUGH
ORDER BY revenue DESC;


-- ====================================================================
-- Widget: Trip duration trend (30 days)
-- ====================================================================
SELECT
    PICKUP_DATE,
    AVG_DURATION_MIN,
    AVG_DISTANCE,
    TRIP_COUNT
FROM FCT_TRIPS_DAILY
WHERE PICKUP_DATE >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY PICKUP_DATE;


-- ====================================================================
-- Widget: Top 10 busiest pickup zones
-- ====================================================================
SELECT
    PICKUP_BOROUGH,
    PICKUP_ZONE,
    SUM(TRIP_COUNT) AS trips,
    SUM(REVENUE) AS revenue
FROM AGG_ZONE_STATS
WHERE PICKUP_DATE >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY PICKUP_BOROUGH, PICKUP_ZONE
ORDER BY trips DESC
LIMIT 10;


-- ====================================================================
-- Widget: Payment type distribution
-- ====================================================================
SELECT
    PAYMENT_METHOD,
    SUM(TRIP_COUNT) AS trips,
    SUM(REVENUE) AS revenue,
    100.0 * SUM(TRIP_COUNT) / SUM(SUM(TRIP_COUNT)) OVER () AS pct_of_trips
FROM AGG_PAYMENT_BREAKDOWN
WHERE PICKUP_DATE >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY PAYMENT_METHOD
ORDER BY trips DESC;


-- ====================================================================
-- Ad-hoc: Hot routes with unusual fare patterns
-- (routes where avg fare is > 2x the borough median)
-- This isn't on the dashboard yet but it's a useful analysis query
-- ====================================================================
WITH borough_medians AS (
    SELECT
        PICKUP_BOROUGH,
        MEDIAN(AVG_FARE) AS median_fare
    FROM AGG_TOP_ROUTES
    GROUP BY PICKUP_BOROUGH
)
SELECT
    r.PICKUP_BOROUGH,
    r.PICKUP_ZONE,
    r.DROPOFF_ZONE,
    r.TRIP_COUNT,
    r.AVG_FARE,
    m.median_fare,
    r.AVG_FARE / m.median_fare AS fare_ratio
FROM AGG_TOP_ROUTES r
JOIN borough_medians m ON r.PICKUP_BOROUGH = m.PICKUP_BOROUGH
WHERE r.AVG_FARE > 2 * m.median_fare
  AND r.TRIP_COUNT >= 50
ORDER BY fare_ratio DESC;

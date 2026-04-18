-- Snowflake DDL for the taxi pipeline gold tables.
--
-- Note: I'm using VARCHAR instead of TEXT in a few places because Snowflake's
-- text performance on filter clauses is sometimes worse than VARCHAR.
-- Probably premature optimization but it's a habit at this point.

USE DATABASE TAXI_PIPELINE;
USE SCHEMA PUBLIC;

-- --------------------------------------------------------------------
-- FCT_TRIPS_HOURLY: hourly rollup by pickup borough
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FCT_TRIPS_HOURLY (
    PICKUP_HOUR_TS       TIMESTAMP_NTZ,
    PICKUP_BOROUGH       VARCHAR(50),
    TRIP_COUNT           NUMBER(38, 0),
    TOTAL_REVENUE        NUMBER(18, 2),
    AVG_DISTANCE         NUMBER(10, 4),
    AVG_DURATION_MIN     NUMBER(10, 4),
    AVG_FARE             NUMBER(10, 4),
    TOTAL_PASSENGERS     NUMBER(38, 0),
    AVG_REVENUE_PER_TRIP NUMBER(18, 4)
);

-- --------------------------------------------------------------------
-- FCT_TRIPS_DAILY: one row per day
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FCT_TRIPS_DAILY (
    PICKUP_DATE          DATE,
    TRIP_COUNT           NUMBER(38, 0),
    TOTAL_REVENUE        NUMBER(18, 2),
    TOTAL_TIPS           NUMBER(18, 2),
    AVG_DISTANCE         NUMBER(10, 4),
    AVG_DURATION_MIN     NUMBER(10, 4),
    UNIQUE_PICKUP_ZONES  NUMBER(38, 0)
);

-- --------------------------------------------------------------------
-- AGG_TOP_ROUTES: top pickup->dropoff routes, ranked per borough
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AGG_TOP_ROUTES (
    PICKUP_BOROUGH       VARCHAR(50),
    PICKUP_ZONE          VARCHAR(100),
    DROPOFF_BOROUGH      VARCHAR(50),
    DROPOFF_ZONE         VARCHAR(100),
    TRIP_COUNT           NUMBER(38, 0),
    AVG_FARE             NUMBER(10, 4),
    AVG_DURATION_MIN     NUMBER(10, 4),
    RANK_WITHIN_BOROUGH  NUMBER(10, 0)
);

-- --------------------------------------------------------------------
-- AGG_ZONE_STATS: per-zone daily stats w/ 7-day rolling avg
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AGG_ZONE_STATS (
    PICKUP_DATE          DATE,
    PICKUP_BOROUGH       VARCHAR(50),
    PICKUP_ZONE          VARCHAR(100),
    TRIP_COUNT           NUMBER(38, 0),
    REVENUE              NUMBER(18, 2),
    TRIP_COUNT_7D_AVG    NUMBER(18, 4),
    REVENUE_7D_AVG       NUMBER(18, 4)
);

-- --------------------------------------------------------------------
-- AGG_PAYMENT_BREAKDOWN: payment method mix per day
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AGG_PAYMENT_BREAKDOWN (
    PICKUP_DATE          DATE,
    PAYMENT_METHOD       VARCHAR(30),
    TRIP_COUNT           NUMBER(38, 0),
    REVENUE              NUMBER(18, 2)
);

-- clustering keys for the big tables
-- note: only useful once tables have many millions of rows
ALTER TABLE FCT_TRIPS_HOURLY CLUSTER BY (PICKUP_HOUR_TS);
ALTER TABLE AGG_ZONE_STATS CLUSTER BY (PICKUP_DATE, PICKUP_BOROUGH);

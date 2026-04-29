{{ config(
    materialized='table',
    pre_hook="{{ create_fact_trips_monthly() }}"
) }}

SELECT
    year,
    month,
    taxi_type,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    ROUND(SUM(trip_distance), 2) AS total_distance,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(SUM(tip_amount), 2) AS total_tips,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip,
    ROUND(AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)), 2) AS avg_trip_duration_minutes
FROM {{ ref('fact_trips') }}
GROUP BY year, month, taxi_type
ORDER BY year, month, taxi_type
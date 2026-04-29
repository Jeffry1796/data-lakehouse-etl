{{ config(
    materialized='table',
    pre_hook="{{ create_fact_trips_by_payment() }}"
) }}

SELECT
    year,
    month,
    taxi_type,
    case
        when payment_type is null then 7
        else payment_type
    end payment_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM {{ ref('fact_trips') }}
GROUP BY year, month, taxi_type, payment_type
ORDER BY year, month, taxi_type, payment_type
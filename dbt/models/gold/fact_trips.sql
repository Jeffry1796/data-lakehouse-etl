{{ config(
    materialized='incremental',
    unique_key=['trip_id'],
    incremental_strategy='merge',
    partition_by={
        "field": "pickup_date",
        "data_type": "date",
        "granularity": "month"
    },
    pre_hook="{{ create_fact_trips() }}"
) }}

WITH yellow AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pu_location_id', 'do_location_id']) }} AS trip_id,
        'yellow' AS taxi_type,
        vendor_id,
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        DATE(tpep_pickup_datetime) AS pickup_date,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        congestion_surcharge,
        airport_fee,
        NULL AS ehail_fee,
        NULL AS trip_type,
        year,
        month,
        current_timestamp() AS created_at
    FROM {{ ref('yellow_taxi_staging') }}
),

green AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'pu_location_id', 'do_location_id']) }} AS trip_id,
        'green' AS taxi_type,
        vendor_id,
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        DATE(lpep_pickup_datetime) AS pickup_date,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        congestion_surcharge,
        NULL AS airport_fee,
        ehail_fee,
        trip_type,
        year,
        month,
        current_timestamp() AS created_at
    FROM {{ ref('green_taxi_staging') }}
)

SELECT * FROM yellow
UNION ALL
SELECT * FROM green

{% if is_incremental() %}
WHERE (year * 100 + month) > (
    {% if var('start_year_month', none) is not none %}
        {{ var('start_year_month') }}
    {% else %}
        (SELECT MAX(year * 100 + month) FROM {{ this }})
    {% endif %}
)
{% endif %}
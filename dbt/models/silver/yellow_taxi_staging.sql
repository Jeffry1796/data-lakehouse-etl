{{ config(
    materialized='incremental',
    unique_key=['vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pu_location_id', 'do_location_id'],
    incremental_strategy='merge',
    pre_hook="{{ create_yellow_taxi_staging() }}"
) }}

WITH raww AS (
    SELECT
        *
    FROM 
        {{ source('bronze', 'yellow_taxi_raw') }}

    {% if is_incremental() %}
    WHERE (year * 100 + month) > (
        {% if var('start_year_month', none) is not none %}
            {{ var('start_year_month') }}
        {% else %}
            (SELECT MAX(year * 100 + month) FROM {{ this }})
        {% endif %}
    )
    {% endif %}
), netted AS (
    SELECT
        VendorID AS vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        store_and_fwd_flag,
        RatecodeID AS ratecode_id,
        PULocationID AS pu_location_id,
        DOLocationID AS do_location_id,
        SUM(passenger_count) AS passenger_count,
        SUM(trip_distance) AS trip_distance,
        SUM(fare_amount) AS fare_amount,
        SUM(extra) AS extra,
        SUM(mta_tax) AS mta_tax,
        SUM(tip_amount) AS tip_amount,
        SUM(tolls_amount) AS tolls_amount,
        SUM(improvement_surcharge) AS improvement_surcharge,
        SUM(total_amount) AS total_amount,
        payment_type,
        SUM(congestion_surcharge)  AS congestion_surcharge,
        SUM(airport_fee) AS airport_fee,
        year,
        month,
        current_timestamp() AS created_at
    FROM raww
    GROUP BY ALL
)

SELECT 
    * 
FROM 
    netted
WHERE
    trip_distance > 0
    and total_amount > 0
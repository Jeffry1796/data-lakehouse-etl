{% macro create_green_taxi_staging() %}
    CREATE TABLE IF NOT EXISTS {{ target.project }}.data_project_silver.green_taxi_staging (
        vendor_id INT64,
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        store_and_fwd_flag STRING,
        ratecode_id INT64,
        pu_location_id INT64,
        do_location_id INT64,
        passenger_count INT64,
        trip_distance FLOAT64,
        fare_amount FLOAT64,
        extra FLOAT64,
        mta_tax FLOAT64,
        tip_amount FLOAT64,
        tolls_amount FLOAT64,
        ehail_fee FLOAT64,
        improvement_surcharge FLOAT64,
        total_amount FLOAT64,
        payment_type INT64,
        trip_type INT64,
        congestion_surcharge FLOAT64,
        year INT64,
        month INT64,
        created_at TIMESTAMP
    )
{% endmacro %}

{% macro create_yellow_taxi_staging() %}
    CREATE TABLE IF NOT EXISTS {{ target.project }}.data_project_silver.yellow_taxi_staging (
        vendor_id INT64,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        store_and_fwd_flag STRING,
        ratecode_id INT64,
        pu_location_id INT64,
        do_location_id INT64,
        passenger_count INT64,
        trip_distance FLOAT64,
        fare_amount FLOAT64,
        extra FLOAT64,
        mta_tax FLOAT64,
        tip_amount FLOAT64,
        tolls_amount FLOAT64,
        improvement_surcharge FLOAT64,
        total_amount FLOAT64,
        payment_type INT64,
        congestion_surcharge FLOAT64,
        airport_fee FLOAT64,
        year INT64,
        month INT64,
        created_at TIMESTAMP
    )
{% endmacro %}

{% macro create_fact_trips() %}
    CREATE TABLE IF NOT EXISTS {{ target.project }}.data_project_gold.fact_trips (
        trip_id STRING,
        taxi_type STRING,
        vendor_id INT64,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        pickup_date DATE,
        pu_location_id INT64,
        do_location_id INT64,
        passenger_count INT64,
        trip_distance FLOAT64,
        fare_amount FLOAT64,
        extra FLOAT64,
        mta_tax FLOAT64,
        tip_amount FLOAT64,
        tolls_amount FLOAT64,
        improvement_surcharge FLOAT64,
        total_amount FLOAT64,
        payment_type INT64,
        congestion_surcharge FLOAT64,
        airport_fee FLOAT64,
        ehail_fee FLOAT64,
        trip_type INT64,
        year INT64,
        month INT64,
        created_at TIMESTAMP
    )
    PARTITION BY DATE_TRUNC(pickup_date, MONTH)
{% endmacro %}

{% macro create_fact_trips_by_payment() %}
    CREATE TABLE IF NOT EXISTS {{ target.project }}.data_project_gold.fact_trips_by_payment (
        payment_type INT64,
        total_amount FLOAT64,
        trip_count INT64,
        year INT64,
        month INT64
    )
{% endmacro %}

{% macro create_fact_trips_monthly() %}
    CREATE TABLE IF NOT EXISTS {{ target.project }}.data_project_gold.fact_trips_monthly (
        pickup_date DATE,
        total_amount FLOAT64,
        trip_count INT64,
        year INT64,
        month INT64
    )
{% endmacro %}
-- models/dimensions/dim_hesa_delivery.sql
-- Create dimension of HESA file deliveries
{{ config(
    materialized='table',
    unique_key='dim_hesa_delivery_key',
    tags=['dimension', 'hesa', 'delivery'])
    }}

    WITH source_data AS (
        SELECT
            delivery_code,
            delivery_received,
            delivery_version,
            delivery_current,
            collection_ref,
            collection_sent,
            delivery_description,
            source_file
        FROM {{ ref('stage_hesa_delivery_metadata') }}
    )

    SELECT
        CONCAT('DEL_', delivery_code) as dim_hesa_delivery_key,
        delivery_code, -- Business key of HESA delivery
        delivery_received,
        delivery_version,
        delivery_current,
        collection_ref,
        collection_sent,
        delivery_description,
        source_file -- Original CSV filename
    FROM source_data

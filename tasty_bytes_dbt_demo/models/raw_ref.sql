{{ config(
    materialized='incremental',
    database='TASTY_BYTES_DBT_DB',
    schema='BRONZE_RAW',
    alias='T_JYUTYU'
) }}

SELECT *
FROM {{ ref('stg_ref') }}

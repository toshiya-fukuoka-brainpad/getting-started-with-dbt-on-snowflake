{{ config(
    materialized='incremental',
    database='BRONZE',
    schema='RAW',
    alias='test_oracle_datetime',
    incremental_strategy='append'
) }}

SELECT *
FROM BRONZE.STG.test_oracle_datetime

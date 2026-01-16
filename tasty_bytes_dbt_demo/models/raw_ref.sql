{{ config(
    materialized='incremental',
    database='BRONZE',
    schema='RAW',
    alias='T_JYUTYU',
    incremental_strategy='append'
) }}

SELECT *
FROM BRONZE.STG.T_JYUTYU

{{ config(
    materialized='incremental',
    database='BRONZE',
    schema='RAW',
    alias='T_JYUTYU'
) }}

SELECT *
FROM BRONZE.STG.JYUTYU

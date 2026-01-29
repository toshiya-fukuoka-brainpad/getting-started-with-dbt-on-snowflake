{{ config(
    materialized='incremental',
    database='SILVER',
    schema='CLN',
    alias='TEST_ORACLE_DATETIME',
    incremental_strategy='append'
) }}

select
id,
CONVERT_TIMEZONE('Asia/Tokyo','UTC',ORACLE_DATE::TIMESTAMP_NTZ)::TIMESTAMP_TZ as oracle_date,
CONVERT_TIMEZONE('Asia/Tokyo','UTC',ORACLE_TS::TIMESTAMP_NTZ)::TIMESTAMP_TZ as oracle_ts,
CONVERT_TIMEZONE('Asia/Tokyo','UTC',ORACLE_TS_6::TIMESTAMP_NTZ)::TIMESTAMP_TZ as oracle_ts_6,
CONVERT_TIMEZONE('Asia/Tokyo','UTC',ORACLE_TS_9::TIMESTAMP_NTZ)::TIMESTAMP_TZ as oracle_ts_9,
CURRENT_TIMESTAMP() as UPDATE_TIME
FROM {{ ref('oracle_datetime_ref') }}

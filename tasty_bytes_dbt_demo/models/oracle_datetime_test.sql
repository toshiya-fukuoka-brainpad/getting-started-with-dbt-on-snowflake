tags=['oracle', 'incremental']
{{ config(
    materialized='incremental',
    database='SILVER',
    schema='CLN',
    alias='TEST_ORACLE_DATETIME',
    incremental_strategy='append',
    pre_hook=[
        "ALTER SESSION SET TIMEZONE = 'Asia/Tokyo'"
    ],
    tags=['oracle', 'incremental']
) }}

select  
{{ convert_tz('jst_ntz_date', source_tz='Asia/Tokyo', target_tz='Asia/Tokyo') }} as jst_ntz_date,
{{ convert_tz('jst_ntz_timestamp', source_tz='Asia/Tokyo', target_tz='Asia/Tokyo') }} as jst_ntz_timestamp,
{{ convert_tz('jst_tz_timestamp_tz', source_tz='Asia/Tokyo', target_tz='Asia/Tokyo') }} as jst_tz_timestamp_tz,
{{ convert_tz('jst_ltz_timestamp_ltz', source_tz='Asia/Tokyo', target_tz='Asia/Tokyo') }} as jst_ltz_timestamp_ltz,
{{ convert_tz('utc_ntz_date', source_tz='UTC', target_tz='Asia/Tokyo') }} as utc_ntz_date,
{{ convert_tz('utc_ntz_timestamp', source_tz='UTC', target_tz='Asia/Tokyo') }} as utc_ntz_timestamp,
{{ convert_tz('utc_tz_timestamp_tz', source_tz='UTC', target_tz='Asia/Tokyo') }} as utc_tz_timestamp_tz,
{{ convert_tz('utc_ltz_timestamp_ltz', source_tz='UTC', target_tz='Asia/Tokyo') }} as utc_ltz_timestamp_ltz,
CURRENT_TIMESTAMP()  as  UPDATE_DATE
from BRONZE.RAW.TEST_ORACLE_DATETIME

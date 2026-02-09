{{ config(
    materialized='table',
    cluster_by=['ANKEN_NO'],
    post_hook=[
      "{{ setup_keiyaku_policy() }}"
    ]
) }}

SELECT *
FROM {{ target.database }}.{{ target.schema }}.T_ANKEN_SOURCE

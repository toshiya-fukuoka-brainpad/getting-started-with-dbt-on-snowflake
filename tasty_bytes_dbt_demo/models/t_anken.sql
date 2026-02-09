{{ config(
    materialized='table',
    cluster_by=['ANKEN_NO'],
    tags=['keiyaku_model']
    post_hook=[
      "{{ post_hook_keiyaku_policy() }}"
    ]
) }}

SELECT *
FROM {{ target.database }}.{{ target.schema }}.T_ANKEN_SOURCE

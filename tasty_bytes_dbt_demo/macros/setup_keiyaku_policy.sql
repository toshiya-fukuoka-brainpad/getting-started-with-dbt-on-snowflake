{% macro setup_keiyaku_policy() %}
{% if execute %}

  {# 実行環境情報の取得 #}
  {% set current_database = target.database %}
  {% set current_schema = target.schema %}
  {% set target_table = 'T_ANKEN' %}

  -- 1. MEMOIZABLE 関数の作成 (存在しない場合のみ)
  {% set func_exists_query %}
    SELECT COUNT(*) 
    FROM {{ current_database }}.INFORMATION_SCHEMA.FUNCTIONS
    WHERE FUNCTION_NAME = 'GET_KEIYAKU_ANKEN_ARRAY'
      AND FUNCTION_SCHEMA = '{{ current_schema }}'
  {% endset %}
  
  {% if run_query(func_exists_query).columns[0][0] == 0 %}
    CREATE OR REPLACE FUNCTION {{ current_database }}.{{ current_schema }}.get_keiyaku_anken_array()
    RETURNS ARRAY
    MEMOIZABLE
    AS
    $$
      SELECT ARRAY_AGG(DISTINCT ANKEN_NO) FROM {{ current_database }}.{{ current_schema }}.T_KEIYAKU
    $$;
  {% endif %}

  -- 2. Row Access Policy の作成 (存在しない場合のみ)
  {% set policy_exists_query %}
    SELECT COUNT(*)
    FROM {{ current_database }}.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
    WHERE POLICY_NAME = 'FILTER_BY_T_KEIYAKU'
      AND POLICY_SCHEMA = '{{ current_schema }}'
  {% endset %}

  {% if run_query(policy_exists_query).columns[0][0] == 0 %}
    CREATE OR REPLACE ROW ACCESS POLICY {{ current_database }}.{{ current_schema }}.filter_by_T_KEIYAKU
    AS (val_ANKEN_NO VARCHAR) RETURNS BOOLEAN ->
      CURRENT_ROLE() = 'ACCOUNTADMIN'
      OR ARRAY_CONTAINS(val_ANKEN_NO::VARIANT, {{ current_database }}.{{ current_schema }}.get_keiyaku_anken_array());
  {% endif %}

  -- 3. テーブルへのポリシー適用 (未適用の場合のみ)
  {% set table_policy_query %}
    SELECT COUNT(*)
    FROM {{ current_database }}.INFORMATION_SCHEMA.APPLICABLE_POLICIES
    WHERE OBJECT_NAME = '{{ target_table }}'
      AND POLICY_NAME = 'FILTER_BY_T_KEIYAKU'
      AND OBJECT_SCHEMA = '{{ current_schema }}'
  {% endset %}

  {% if run_query(table_policy_query).columns[0][0] == 0 %}
    ALTER TABLE {{ current_database }}.{{ current_schema }}.{{ target_table }}
    ADD ROW ACCESS POLICY {{ current_database }}.{{ current_schema }}.filter_by_T_KEIYAKU ON (ANKEN_NO);
  {% endif %}

  -- 4. クラスタリングの適用 (未設定の場合のみ)
  {% set cluster_info_query %}
    SELECT COUNT(*)
    FROM {{ current_database }}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = '{{ target_table }}'
      AND TABLE_SCHEMA = '{{ current_schema }}'
      AND CLUSTERING_KEY IS NOT NULL
  {% endset %}

  {% if run_query(cluster_info_query).columns[0][0] == 0 %}
    ALTER TABLE {{ current_database }}.{{ current_schema }}.{{ target_table }} CLUSTER BY (ANKEN_NO);
  {% endif %}

{% endif %}
{% endmacro %}

{% macro load_parquet(database, schema, table, stage_path) %}

  {# フルテーブル名を作る #}
  {% set full_table = database ~ '.' ~ schema ~ '.' ~ table %}

  {# ① TRUNCATE TABLE #}
  {% set sql_truncate %}
    TRUNCATE TABLE {{ full_table }};
  {% endset %}
  {% do run_query(sql_truncate) %}

  {# ② COPY INTO #}
  {% set sql_copy %}
    COPY INTO {{ full_table }}
    FROM '{{ stage_path }}'
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'ABORT_STATEMENT';
  {% endset %}
  {% do run_query(sql_copy) %}

  {# ③ INGEST_TIMESTAMP 更新 #}
  {% set sql_ingest %}
    UPDATE {{ full_table }}
    SET INGEST_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE INGEST_TIMESTAMP IS NULL;
  {% endset %}
  {% do run_query(sql_ingest) %}

{% endmacro %}

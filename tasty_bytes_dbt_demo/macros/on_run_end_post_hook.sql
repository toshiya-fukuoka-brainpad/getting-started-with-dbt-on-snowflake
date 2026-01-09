{% macro on_run_end_post_hook(node_name) %}
  INSERT INTO TASTY_BYTES_DBT_DB.MONITORING.DBT_RUN_RESULTS
  (
    RUN_ID,
    RUN_AT,
    TARGET,
    NODE_NAME,
    RESOURCE_TYPE,
    DATABASE_NAME,
    SCHEMA_NAME,
    STATUS,
    EXECUTION_TIME_S,
    ERROR_MESSAGE
  )
  VALUES
  (
    '{{ invocation_id }}',
    CURRENT_TIMESTAMP(),
    '{{ target.name }}',
    '{{ node_name }}',
    'model',                             -- post_hook はモデル単位なので固定
    '{{ database }}',
    '{{ schema }}',
    'success',                            -- モデルが post_hook に到達した時点では成功
    0,
    NULL
  );
{% endmacro %}

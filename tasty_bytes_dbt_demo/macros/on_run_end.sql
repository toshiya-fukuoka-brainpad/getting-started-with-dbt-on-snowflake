{% macro on_run_end(results) %}
  -- 実行ユーザーが書き込み権限を持っているか、実行時にロールが正しいか確認してください
  {% set run_id = invocation_id %}
  {% set target_name = target.name %}

  {% for r in results %}
    -- エラーメッセージの取得と整形
    -- 1. メッセージがない場合はNone
    -- 2. メッセージがある場合は、SQL壊れを防ぐためにドル記号($)を除去し、先頭1000文字にカット（念のため）
    {% set error_msg = none %}
    {% if r.status != 'success' and r.message %}
      {% set error_msg = r.message | replace('$', '') | truncate(1000) %}
    {% endif %}

    {% set sql %}
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
        '{{ run_id }}',
        CURRENT_TIMESTAMP(),
        '{{ target_name }}',
        '{{ r.node.name }}',
        '{{ r.node.resource_type }}',
        '{{ r.node.database }}',
        '{{ r.node.schema }}',
        '{{ r.status }}',
        {{ r.execution_time | default(0) }},
        {% if error_msg %}
          -- ドル引用符($$)を使うことで、メッセージ内のシングルクォートによる構文エラーを回避
          $$ {{ error_msg }} $$
        {% else %}
          NULL
        {% endif %}
      );
    {% endset %}

    -- クエリの実行
    {% do run_query(sql) %}
  {% endfor %}

  -- ログの書き込みを確定させる（念のための処理）
  {% do run_query("COMMIT;") %}

{% endmacro %}

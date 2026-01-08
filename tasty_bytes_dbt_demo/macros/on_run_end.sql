{% macro on_run_end(results) %}

  {% set run_id = invocation_id %}
  {% set target_name = target.name %}

  {% for r in results %}

    {% set error_msg = r.message if r.status != 'success' else none %}

    {% set sql %}
      INSERT INTO MONITORING.DBT_RUN_RESULTS
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
          $$ {{ error_msg | replace("'", "''") }} $$
        {% else %}
          NULL
        {% endif %}
      );
    {% endset %}

    {% do run_query(sql) %}

  {% endfor %}

{% endmacro %}

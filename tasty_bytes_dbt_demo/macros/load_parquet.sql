-- macros/load_parquet.sql
{% macro load_parquet(database, schema, table, stage_path) %}
    CALL {{ database }}.{{ schema }}.LOAD_PARQUET_TABLE(
        '{{ database }}',
        '{{ schema }}',
        '{{ table }}',
        '{{ stage_path }}'
    );
{% endmacro %}

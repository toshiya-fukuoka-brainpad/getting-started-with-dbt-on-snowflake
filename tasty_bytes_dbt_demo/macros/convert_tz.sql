{% macro convert_tz(column_name, source_tz='UTC',target_tz='Asia/Tokyo') %}
    CONVERT_TIMEZONE(
        '{{ source_tz }}', 
        '{{ target_tz }}', 
        {{ column_name }}::TIMESTAMP_NTZ
    )::TIMESTAMP_TZ
{% endmacro %}

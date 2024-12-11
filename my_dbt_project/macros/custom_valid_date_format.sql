{% test custom_valid_date_format(model, source_name, table_name, column_name) %}
SELECT
    {{ column_name }} AS failing_value
FROM {{ source(source_name, table_name) }}
WHERE TRY_TO_DATE({{ column_name }}) IS NULL
{% endtest %}

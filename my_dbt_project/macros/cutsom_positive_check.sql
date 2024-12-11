{% test custom_positive_check(model, source_name, table_name, column_name) %}
SELECT
    {{ column_name }} AS failing_value
FROM {{ source(source_name, table_name) }}
WHERE {{ column_name }} <= 0
{% endtest %}

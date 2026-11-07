{% macro air_quality_category(value_column) %}

case
    when {{ value_column }} < 50 then 'Good'
    when {{ value_column }} < 100 then 'Moderate'
    when {{ value_column }} < 150 then 'Unhealthy for Sensitive'
    when {{ value_column }} < 200 then 'Unhealthy'
    else 'Very Unhealthy'
end

{% endmacro %}

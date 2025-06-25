
{% test warn_if_nonempty(model) %}

SELECT *
FROM {{ model }}

{% endtest %}

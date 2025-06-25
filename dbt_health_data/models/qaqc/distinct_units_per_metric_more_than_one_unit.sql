
SELECT *
FROM {{ref('distinct_units_per_metric')}}
WHERE distinct_units > 1

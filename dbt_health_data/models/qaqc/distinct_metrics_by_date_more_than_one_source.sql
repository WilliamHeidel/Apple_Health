
SELECT *
FROM {{ref('distinct_metrics_by_date')}}
WHERE distinct_sources > 1

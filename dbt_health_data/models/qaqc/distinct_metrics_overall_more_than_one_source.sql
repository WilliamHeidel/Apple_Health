
SELECT *
FROM {{ref('distinct_metrics_overall')}}
WHERE distinct_sources > 1

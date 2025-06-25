
WITH groupings AS (
    SELECT name, units, source
    FROM {{ref('distinct_metrics_by_date')}}
    GROUP BY name, units, source
),

with_distinct_sources AS (
    SELECT *, COUNT(*) OVER(PARTITION BY name, units) AS distinct_sources
    FROM groupings
)

SELECT *
FROM with_distinct_sources

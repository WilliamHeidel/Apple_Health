
WITH groupings AS (
    SELECT date, name, units, source
    FROM {{ref('gold_health_data')}}
    GROUP BY date, name, units, source
),

with_distinct_sources AS (
    SELECT *, COUNT(*) OVER(PARTITION BY date, name, units) AS distinct_sources
    FROM groupings
)

SELECT *
FROM with_distinct_sources

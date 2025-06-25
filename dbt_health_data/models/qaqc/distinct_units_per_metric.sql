
WITH groupings AS (
    SELECT DISTINCT name, units
    FROM {{ref('gold_health_data')}}
),

distinct_units_per_metric AS (
    SELECT *, COUNT(*) OVER(PARTITION BY name) AS distinct_units
    FROM groupings
)

SELECT *
FROM distinct_units_per_metric

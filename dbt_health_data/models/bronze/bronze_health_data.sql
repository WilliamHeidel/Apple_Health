
WITH joined AS (
SELECT l1.source_file, l2.units, l2.name, l3.*
, ROW_NUMBER() OVER (PARTITION BY l2.units, l2.name, l3.date, l3.source ORDER BY l1.source_file DESC) AS row_num
FROM {{source('apple_health','read_json')}} l1
RIGHT JOIN {{source('apple_health','read_json__data__metrics')}} l2
    ON l2._dlt_parent_id = l1._dlt_id
FULL OUTER JOIN {{source('apple_health','read_json__data__metrics__data')}} l3
    ON l3._dlt_parent_id = l2._dlt_id
),

filtered AS (
SELECT *
FROM joined
WHERE row_num = 1
)

SELECT *
FROM filtered

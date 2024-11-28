WITH ranked_data AS (
    SELECT *,
        ("Close" - LAG("Close") OVER (PARTITION BY name, fullname ORDER BY date)) / LAG("Close") OVER (PARTITION BY name, fullname ORDER BY date) * 100 AS change,
       -- 한 시간 변화율 계산
        (LEAD("Close", 1) OVER (PARTITION BY name, fullname ORDER BY date) - "Close") 
        / "Close" * 100 AS hour_change,
        -- 하루 변화율 계산
        (LEAD("Close", 9) OVER (PARTITION BY name, fullname ORDER BY date) - "Close") 
        / "Close" * 100 AS i_day_change_mean,
        
        -- 3일 변화율 계산
        (LEAD("Close", 27) OVER (PARTITION BY name, fullname ORDER BY date) - "Close") 
        / "Close" * 100 AS three_day_change,
        
        -- 일주일 변화율 계산
        (LEAD("Close", 45) OVER (PARTITION BY name, fullname ORDER BY date) - "Close") 
        / "Close" * 100 AS weekly_change
    FROM {{ ref("src_foreign_stock_change_rate_conditions") }}
),
aggregated_data AS (
    SELECT
        CASE
            WHEN change > 10 THEN 'Change > 10'
            WHEN change > 5 THEN 'Change > 5'
            WHEN change <= -10 THEN 'Change <= -10'
            WHEN change <= -5 THEN 'Change <= -5'
            ELSE 'Other'
        END AS condition,
        AVG(hour_change) AS hour_change_mean,
        AVG(i_day_change_mean) AS i_day_change_mean,
        AVG(three_day_change) AS three_day_change_mean,
        AVG(weekly_change) AS weekly_change_mean,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hour_change) AS hour_change_median,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i_day_change_mean) AS i_day_change_median,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY three_day_change) AS three_day_change_median,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY weekly_change) AS weekly_change_median
    FROM ranked_data
    WHERE change > 5 OR change <= -5
    GROUP BY condition
)

-- mean값
SELECT 
    'hour_change_mean' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN hour_change_mean ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN hour_change_mean ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN hour_change_mean ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN hour_change_mean ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

UNION ALL

SELECT 
    'i_day_change_mean' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN i_day_change_mean ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN i_day_change_mean ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN i_day_change_mean ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN i_day_change_mean ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

UNION ALL

SELECT 
    'three_day_change_mean' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN three_day_change_mean ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN three_day_change_mean ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN three_day_change_mean ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN three_day_change_mean ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

UNION ALL

SELECT 
    'weekly_change_mean' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN weekly_change_mean ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN weekly_change_mean ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN weekly_change_mean ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN weekly_change_mean ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

-- median값
UNION ALL

SELECT 
    'hour_change_median' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN hour_change_median ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN hour_change_median ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN hour_change_median ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN hour_change_median ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

UNION ALL

SELECT 
    'i_day_change_median' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN i_day_change_median ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN i_day_change_median ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN i_day_change_median ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN i_day_change_median ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

UNION ALL

SELECT 
    'three_day_change_median' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN three_day_change_median ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN three_day_change_median ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN three_day_change_median ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN three_day_change_median ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

UNION ALL

SELECT 
    'weekly_change_median' AS metric, 
    MAX(CASE WHEN condition = 'Change > 10' THEN weekly_change_median ELSE NULL END) AS "Change > 10",
    MAX(CASE WHEN condition = 'Change > 5' THEN weekly_change_median ELSE NULL END) AS "Change > 5",
    MAX(CASE WHEN condition = 'Change <= -10' THEN weekly_change_median ELSE NULL END) AS "Change <= -10",
    MAX(CASE WHEN condition = 'Change <= -5' THEN weekly_change_median ELSE NULL END) AS "Change <= -5"
FROM aggregated_data
GROUP BY metric

-- revenue_by_month_year.sql
-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).


WITH monthly_revenue AS (
    SELECT 
        CAST(strftime('%m', o.order_purchase_timestamp) AS INTEGER) as month_no,
        CASE strftime('%m', o.order_purchase_timestamp)
            WHEN '01' THEN 'Ene'
            WHEN '02' THEN 'Feb'
            WHEN '03' THEN 'Mar'
            WHEN '04' THEN 'Abr'
            WHEN '05' THEN 'May'
            WHEN '06' THEN 'Jun'
            WHEN '07' THEN 'Jul'
            WHEN '08' THEN 'Ago'
            WHEN '09' THEN 'Sep'
            WHEN '10' THEN 'Oct'
            WHEN '11' THEN 'Nov'
            WHEN '12' THEN 'Dec'
        END as month,
        strftime('%Y', o.order_purchase_timestamp) as year,
        SUM(oi.price + COALESCE(oi.freight_value, 0)) as revenue
    FROM olist_orders o
    JOIN olist_order_items oi ON o.order_id = oi.order_id
    WHERE strftime('%Y', o.order_purchase_timestamp) IN ('2016', '2017', '2018')
        AND o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY month_no, year
)
SELECT 
    m.month_no,
    m.month,
    COALESCE(r2016.revenue, 0.00) AS Year2016,
    COALESCE(r2017.revenue, 0.00) AS Year2017,
    COALESCE(r2018.revenue, 0.00) AS Year2018
FROM (
    SELECT 1 as month_no, 'Ene' as month
    UNION SELECT 2, 'Feb'
    UNION SELECT 3, 'Mar'
    UNION SELECT 4, 'Abr'
    UNION SELECT 5, 'May'
    UNION SELECT 6, 'Jun'
    UNION SELECT 7, 'Jul'
    UNION SELECT 8, 'Ago'
    UNION SELECT 9, 'Sep'
    UNION SELECT 10, 'Oct'
    UNION SELECT 11, 'Nov'
    UNION SELECT 12, 'Dec'
) m
LEFT JOIN (SELECT month_no, revenue FROM monthly_revenue WHERE year = '2016') r2016 ON m.month_no = r2016.month_no
LEFT JOIN (SELECT month_no, revenue FROM monthly_revenue WHERE year = '2017') r2017 ON m.month_no = r2017.month_no
LEFT JOIN (SELECT month_no, revenue FROM monthly_revenue WHERE year = '2018') r2018 ON m.month_no = r2018.month_no
ORDER BY m.month_no;
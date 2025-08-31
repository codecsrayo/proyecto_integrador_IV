-- real_vs_estimated_delivered_time.sql
-- TODO: Esta consulta devolverá una tabla con las diferencias entre los tiempos 
-- reales y estimados de entrega por mes y año. Tendrá varias columnas: 
-- month_no, con los números de mes del 01 al 12; month, con las primeras 3 letras 
-- de cada mes (ej. Ene, Feb); Year2016_real_time, con el tiempo promedio de 
-- entrega real por mes de 2016 (NaN si no existe); Year2017_real_time, con el 
-- tiempo promedio de entrega real por mes de 2017 (NaN si no existe); 
-- Year2018_real_time, con el tiempo promedio de entrega real por mes de 2018 
-- (NaN si no existe); Year2016_estimated_time, con el tiempo promedio estimado 
-- de entrega por mes de 2016 (NaN si no existe); Year2017_estimated_time, con 
-- el tiempo promedio estimado de entrega por mes de 2017 (NaN si no existe); y 
-- Year2018_estimated_time, con el tiempo promedio estimado de entrega por mes 
-- de 2018 (NaN si no existe).
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Considera tomar order_id distintos.

WITH monthly_delivery_times AS (
    SELECT 
        CAST(strftime('%m', order_purchase_timestamp) AS INTEGER) as month_no,
        CASE strftime('%m', order_purchase_timestamp)
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
        strftime('%Y', order_purchase_timestamp) as year,
        AVG(julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp)) as real_time,
        AVG(julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp)) as estimated_time
    FROM olist_orders
    WHERE order_status = 'delivered' 
        AND order_delivered_customer_date IS NOT NULL
        AND order_estimated_delivery_date IS NOT NULL
        AND strftime('%Y', order_purchase_timestamp) IN ('2016', '2017', '2018')
    GROUP BY month_no, year
)
SELECT 
    m.month_no,
    m.month,
    COALESCE(rt2016.real_time, 'NaN') AS Year2016_real_time,
    COALESCE(rt2017.real_time, 'NaN') AS Year2017_real_time,
    COALESCE(rt2018.real_time, 'NaN') AS Year2018_real_time,
    COALESCE(et2016.estimated_time, 'NaN') AS Year2016_estimated_time,
    COALESCE(et2017.estimated_time, 'NaN') AS Year2017_estimated_time,
    COALESCE(et2018.estimated_time, 'NaN') AS Year2018_estimated_time
FROM (
    SELECT DISTINCT month_no, month FROM monthly_delivery_times
) m
LEFT JOIN (SELECT month_no, real_time FROM monthly_delivery_times WHERE year = '2016') rt2016 ON m.month_no = rt2016.month_no
LEFT JOIN (SELECT month_no, real_time FROM monthly_delivery_times WHERE year = '2017') rt2017 ON m.month_no = rt2017.month_no
LEFT JOIN (SELECT month_no, real_time FROM monthly_delivery_times WHERE year = '2018') rt2018 ON m.month_no = rt2018.month_no
LEFT JOIN (SELECT month_no, estimated_time FROM monthly_delivery_times WHERE year = '2016') et2016 ON m.month_no = et2016.month_no
LEFT JOIN (SELECT month_no, estimated_time FROM monthly_delivery_times WHERE year = '2017') et2017 ON m.month_no = et2017.month_no
LEFT JOIN (SELECT month_no, estimated_time FROM monthly_delivery_times WHERE year = '2018') et2018 ON m.month_no = et2018.month_no
ORDER BY m.month_no;
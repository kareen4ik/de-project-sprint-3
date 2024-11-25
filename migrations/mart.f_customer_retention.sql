DELETE FROM mart.f_customer_retention
WHERE period_id = DATE_PART('week', '{{ ds }}'::DATE);

WITH weekly_sales AS (
    SELECT 
        customer_id,
        item_id,
        DATE_PART('week', date_id::TEXT::DATE) AS week_id,
        SUM(payment_amount) AS total_revenue,
        COUNT(*) AS order_count,
        MAX(order_status) AS status
    FROM mart.f_sales
    GROUP BY customer_id, item_id, DATE_PART('week', date_id::TEXT::DATE)
),

min_order_week AS (
    SELECT 
        customer_id,
        MIN(week_id) AS min_week_id
    FROM 
        weekly_sales
    GROUP BY customer_id
),

new_customers AS (
    SELECT 
        ws.customer_id, 
        ws.week_id, 
        (ws.week_id = mow.min_week_id) AS is_new_customer
    FROM 
        weekly_sales ws
    LEFT JOIN 
        min_order_week mow 
    USING (customer_id)
),

returning_customers AS (
    SELECT 
        ws.customer_id, 
        ws.week_id, 
        (ws.order_count > 1) AS is_returning_customer
    FROM 
        weekly_sales ws
),

refunded_customers AS (
    SELECT 
        customer_id, 
        week_id, 
        (status = 'refunded') AS is_refunded
    FROM 
        weekly_sales
)

INSERT INTO mart.f_customer_retention (
    period_id,
    period_name,
    item_id,
    new_customers_count,
    returning_customers_count,
    refunded_customers_count,
    new_customers_revenue,
    returning_customers_revenue,
    refunded_customers_revenue
)
SELECT 
    ws.week_id AS period_id,
    'weekly' AS period_name,
    ws.item_id,
    COUNT(DISTINCT ws.customer_id) FILTER (WHERE nc.is_new_customer) AS new_customers_count,
    COUNT(DISTINCT ws.customer_id) FILTER (WHERE rc.is_returning_customer) AS returning_customers_count,
    COUNT(DISTINCT ws.customer_id) FILTER (WHERE refunded.is_refunded) AS refunded_customers_count,
    SUM(ws.total_revenue) FILTER (WHERE nc.is_new_customer AND ws.status = 'shipped') AS new_customers_revenue,
    SUM(ws.total_revenue) FILTER (WHERE rc.is_returning_customer AND ws.status = 'shipped') AS returning_customers_revenue,
    SUM(ws.total_revenue) FILTER (WHERE refunded.is_refunded) AS refunded_customers_revenue
FROM weekly_sales ws
LEFT JOIN 
    new_customers nc USING (customer_id, week_id)
LEFT JOIN 
    returning_customers rc USING (customer_id, week_id)
LEFT JOIN 
    refunded_customers refunded USING (customer_id, week_id)
GROUP BY ws.week_id, ws.item_id;
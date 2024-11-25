DELETE FROM mart.f_sales 
WHERE date_id = '{{ ds_nodash }}';

INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, order_status)
SELECT 
    dc.date_id, 
    uol.item_id, 
    uol.customer_id, 
    uol.city_id, 
    uol.quantity, 
    CASE 
        WHEN uol.status = 'refunded' THEN -uol.payment_amount 
        ELSE uol.payment_amount 
    END AS payment_amount,
    COALESCE(uol.status, 'shipped') AS order_status
FROM staging.user_order_log uol
INNER JOIN mart.d_calendar AS dc 
    ON uol.date_time::DATE = dc.date_actual
WHERE uol.date_time::DATE = '{{ds}}';
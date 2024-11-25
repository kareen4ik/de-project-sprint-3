Привет, ревьювер!

Меня зовут Карина, я уже 4 года работаю продуктовым аналитиком. Сейчас учусь на дата инженера, чтобы научиться оптимизации и автоматизации.

Проект кажется не самым сложным и не самым объёмным. Но если я что-то упустила, буду ждать информацию.

#Этап 1.

Сначала добавим колонку для сохранения информации о статусе заказов:

```sql
ALTER TABLE mart.f_sales ADD COLUMN order_status VARCHAR(50);
```

Колонка добавлена. Теперь добавляем условия:

```sql
COALESCE(uol.status, 'shipped') AS order_status
CASE WHEN status = 'refunded' THEN -revenue ELSE revenue END AS revenue
```
COALESCE использую на всякий случай. Мало ли что случится, и статус придёт незаполненным. Мы же по дефолту считаем, что если в отчёте есть заказ, значит, он отгружен.

Нужно проверить данные на адеквтаность:
```sql
SELECT date_id, item_id, order_status, 
	  SUM(payment_amount) AS total_amount FROM mart.f_sales 
GROUP BY 1, 2, 3 
ORDER BY 1, 2;
```

Проверка показала, что для заказов со статусом `refunded` выручка отрицательная, для shipped – положительная. Значит, расчёт работает корректно.


#Этап 2.

Сначала создадим таблицу для анализа возвращаемости клиентов. 

DDL новой таблицы:

```sql 
CREATE TABLE mart.f_customer_retention (
    period_id INT,                        
    period_name VARCHAR(50),              
    item_id INT,                          
    new_customers_count INT,              
    returning_customers_count INT,        
    refunded_customers_count INT,          
    new_customers_revenue NUMERIC(12, 2), 
    returning_customers_revenue NUMERIC(12, 2), 
    refunded_customers_revenue NUMERIC(12, 2)     
);

```

Далее – приступаем к заполнению данных. Нужно определить покупателей (новый / возвращающийся / делал возврат). Сначала в СТЕ буду присваивать статус флагами is_new_customer, is_returning_customer, is_refunded. В финальном SELECT'е посчитаю количество клиентов и сумму выручки по этим флагам.

Скрипт:

```sql
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
```

В `migrations/mart.f_customer_retention.sql` добавляем условие для очищения таблицы и последующей инкрементальной вставки:
```sql
DELETE FROM mart.f_customer_retention
WHERE period_id = DATE_PART('week', '{{ ds }}'::DATE);
```

Обновляем DAG: при помощи PostgresOperator добавляем задачу `update_f_customer_retention`, которая будет выполнять SQL скрипт

```python
    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}}
    )
``` 

Далее – обновляем порядок выполнения задач в DAG. Логично, что скрипт f_customer_retention будет в очереди последним, так как собирается из f_sales:
```python
(
    generate_report
    >> get_report
    >> get_increment
    >> upload_user_order_inc
    >> [update_d_item_table, update_d_city_table, update_d_customer_table]
    >> update_f_sales
    >> update_f_customer_retention
)
```

Также в args прописала свой email, чтобы быть в курсе о возможных падениях
```python
args = {
    "owner": "rinchen.helmut",
    'email': ['rinchen.helmut@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2
}
```

Думаю, в случае разовых проблем с сетью или API, 2х попыток будет достаточно.


#Этап 3.
Для того, чтобы избежать дублей при перезапуске DAG'а, необходимо очищать таблицу:

```sql
DELETE FROM mart.f_sales 
WHERE date_id = '{{ ds_nodash }}';
```
где `'{{ ds_nodash }}'` – параметр для даты формата YYYYMMDD

Запускаем backfill'ом DAG, чтобы убедиться, что за предыдущие даты данные перезапишутся, а не задублируются

```bash
airflow dags backfill sales_mart -s 2024-11-01 -e 2024-11-24
```

DAG отбежал, проблем не выявлено.
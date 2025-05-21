{{
    config (
        materialized = 'incremental',
        unique_key = 'customer_id'
    )
}}

select customer_id,
       COUNT(DISTINCT order_id) AS total_orders,
       SUM(price) AS total_spent,
       current_timestamp as as_of_day
from  {{ ref('orders') }}
JOIN {{ ref('products') }} p USING (product_id)
GROUP BY customer_id
ORDER BY total_spent DESC
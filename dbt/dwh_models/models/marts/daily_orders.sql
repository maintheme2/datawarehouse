{{
    config(
        materialized = 'incremental',
        unique_key='order_date'
    )
}}


with get_prices as (
  SELECT
    product_id,
    price
  FROM {{ ref('products') }}
)
SELECT
  order_date::date AS order_date,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(price) AS total_revenue,
  current_timestamp as as_of_day
FROM {{ ref('orders') }}
JOIN get_prices USING (product_id)
GROUP BY order_date::date
{{
    config(
        materialized = 'incremental',
    )
}}

select stg.order_id,
       stg.order_date,
       stg.customer_id,
       stg.product_id,
       current_timestamp as as_of_day
from store_stg.stg_orders stg
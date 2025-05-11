{{
    config(
        materialized = 'incremental',
    )
}}

select stg.product_id,
       stg.category,
       stg.price,
       current_timestamp as as_of_day
from store_stg.stg_products stg
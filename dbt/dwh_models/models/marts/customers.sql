{{
    config(
        materialized = 'incremental',
    )
}}

select stg.customer_id,
       stg.name,
       stg.address,
       stg.phone,
       stg.email,
       stg.gender,
       stg.date_of_birth,
       current_timestamp as as_of_day
from store_stg.stg_customers stg
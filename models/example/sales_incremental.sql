{{config(
    materialized='incremental',
    unique_key='order_id'
)}}

select
    order_id,
    customer_id,
    amount,
    updated_at
from {{source('raw_data', 'sales')}}

{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
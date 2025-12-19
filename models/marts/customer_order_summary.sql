
with cust as (
    select * from {{ ref('stg_customer') }}
),
ord as (
    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status
    from {{ source('data_feed', 'raw_orders') }}
)

select
    cust.customer_id,
    cust.first_name,
    cust.last_name,
    ord.status,
    min(ord.order_date) as first_order_date,
    max(ord.order_date) as most_recent_order_date,
    count(ord.order_id) as total_orders
from cust
left join ord
    on cust.customer_id = ord.customer_id
group by 1,2,3,4

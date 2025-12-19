with source_data as (
    select
        id as order_id,
        user_id as customer_id,
        order_date
        -- status column is intentionally NOT included
    from {{ source('data_feed', 'raw_orders') }}
)

select * from source_data


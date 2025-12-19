with source_data as (
    select
        id as customer_id,
        first_name,
        last_name
    from {{ source('data_feed', 'raw_customer') }}
)

select * from source_data 

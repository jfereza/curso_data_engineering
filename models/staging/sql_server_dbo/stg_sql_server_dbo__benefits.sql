with 

source as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

source2 as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

interm as (

    select
        A.*,
        B.*
    from source A
    left join source2 B
        on A.order_id=B.order_id

),

renamed as (

    select
        *,
        row_number() over (partition by order_id order by product_id ) as num_products
    from interm

)

select * from renamed
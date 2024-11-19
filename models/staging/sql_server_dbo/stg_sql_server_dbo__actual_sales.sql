with 

source as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

source2 as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

interm as (

    select
        A.product_id,
        A.quantity,
        month(B.created_at)::number as month
    from source A
    left join source2 B
        on A.order_id=B.order_id

),

renamed as (

    select
        product_id,
        sum(quantity) as quantity,
        month
    from interm
    group by 
        1,3
    order by 
        3,1

)

select * from renamed
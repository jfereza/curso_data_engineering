with 

source as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

iterm as (

    select
        distinct nullif(trim(shipping_service), '') as shipping_service
    from 
        source 

),

renamed as (

    select
        shipping_service,
        {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} as shipping_id
    from 
        iterm 

)

select * from renamed

with 

source as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

interm as (
    
    select 
        order_id,
        user_id,
        address_id,
        tracking_id,
        nullif(trim(shipping_service), '') as shipping_service,
        status,
        convert_timezone('UTC', created_at) as created_at_utc,
        convert_timezone('UTC', estimated_delivery_at) as estimated_delivery_at_utc,
        convert_timezone('UTC', delivered_at) as delivered_at_utc,
        order_cost,
        shipping_cost,
        case
            when promo_id is null then 'no discount'
            when nullif(trim(promo_id), '') is null then 'no discount'
            else lower(promo_id)
            end as promo_id,
        order_total,
        _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as _fivetran_synced_utc
        
    from source

),

renamed as (

    select
        order_id,
        user_id,
        address_id,
        tracking_id,
        {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} as shipping_id,
        status,
        created_at_utc,
        estimated_delivery_at_utc,
        delivered_at_utc,
        order_cost,
        shipping_cost,
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id,
        order_total,
        _fivetran_deleted,
        _fivetran_synced_utc

    from 
        interm

)

select * from renamed

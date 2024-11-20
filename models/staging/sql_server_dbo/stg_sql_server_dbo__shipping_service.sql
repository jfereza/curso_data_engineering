{{
  config(
    materialized='table'
  )
}}

with 

source as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

iterm as (

    select
        distinct nullif(trim(shipping_service), '') as shipping_service -- primero cambio los vacios o espacios por null
    from 
        source 

),

renamed as (

    select
        shipping_service,
        case -- cambio el nombre del shipping service por un hash. Si es nulo lo dejo nulo    
            when shipping_service is null then shipping_service
            else {{ dbt_utils.generate_surrogate_key(['shipping_service']) }}
            end as shipping_id
    from 
        iterm 

)

select * from renamed

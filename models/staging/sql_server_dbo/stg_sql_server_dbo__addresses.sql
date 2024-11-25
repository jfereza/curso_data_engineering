{{
  config(
    materialized='table'
  )
}}

with 

source as (

    select * from {{ source('sql_server_dbo', 'addresses') }}

),

renamed as (

    select
        address_id,
        zipcode,
        country,
        address, 
        state,
        convert_timezone('UTC', _fivetran_synced) as _fivetran_synced_utc  -- convierto la zona horaria
    from source

)

select * from renamed

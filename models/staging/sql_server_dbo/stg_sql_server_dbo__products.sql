{{
  config(
    materialized='table'
  )
}}

with 

source as (

    select * from {{ source('sql_server_dbo', 'products') }}

),

renamed as (

    select
        product_id,
        price,
        name as product_name, -- renombro la columna
        inventory,
        _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as _fivetran_synced_UTC -- convierto la zona horaria

    from source

)

select * from renamed

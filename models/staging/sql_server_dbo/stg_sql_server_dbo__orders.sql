{{
  config(
    materialized='table'
  )
}}

with 

source as (

    select * from {{ ref('base_sql_server_dbo__orders') }}

),

renamed as (

    select
        order_id,
        user_id,
        address_id,
        {{ dbt_utils.generate_surrogate_key(['status']) }} as status_id,
        created_at_utc,
        order_cost,
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id,
        order_total,
        _fivetran_deleted,
        _fivetran_synced_utc

    from 
        source

)

select * from renamed

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
        distinct status as status
    from source 

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['status']) }} as status_id,
        status as status
    from 
        iterm 

)

select * from renamed

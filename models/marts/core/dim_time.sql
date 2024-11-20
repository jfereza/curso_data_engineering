{{
  config(
    materialized='table'
  )
}}

with 

source as (

    select * from {{ ref('stg_sql_server_dbo__time') }}

)

select * from source
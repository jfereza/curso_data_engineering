with 

source as (

    select * from {{ source('google_sheets', 'budget') }}

),

renamed as (

    select
        product_id,
        quantity,
        month(month)::number as month_num,
        monthname(month)::varchar(4) as month_name,
        convert_timezone('UTC', _fivetran_synced) as _fivetran_synced_utc,
        _row as row
    from source

)

select * from renamed

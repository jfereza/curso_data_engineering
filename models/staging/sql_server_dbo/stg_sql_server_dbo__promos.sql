with 

source as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

interm as (

    select
        promo_id,
        discount,
        status,
        _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as _fivetran_synced_UTC

    from source

    union

    select 
        'no discount' as promo_id,
        0 as discount,
        'active' as status,
        null as _fivetran_deleted,
        convert_timezone('UTC', current_date) as _fivetran_synced_utc

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id,
        lower(promo_id) as promo_name,
        discount,
        status,
        _fivetran_deleted,
        _fivetran_synced_utc

    from interm
    order by discount ASC

)

select * from renamed

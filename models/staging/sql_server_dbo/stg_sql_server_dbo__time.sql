with 

time_spine as (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="cast('2020-01-01' as date)",  
        end_date="cast('2024-12-31' as date)"
    ) }}
),

renamed as (

    select
        date as time_date,                      -- fecha
        year(date) as year,        -- año
        month(date) as month,      -- mes
        day(date) as day,          -- día
        quarter(date) as quarter,  -- trimestre
        case
            when dayofweek(date) = 1 then 'sunday'
            when dayofweek(date) = 2 then 'monday'
            when dayofweek(date) = 3 then 'tuesday'
            when dayofweek(date) = 4 then 'wednesday'
            when dayofweek(date) = 5 then 'thursday'
            when dayofweek(date) = 6 then 'friday'
            when dayofweek(date) = 7 then 'saturday'
            end as day_of_week,                      -- día de la semana
        case
            when month(date) in (12, 1, 2) then 'q1'
            when month(date) in (3, 4, 5) then 'q2'
            when month(date) in (6, 7, 8) then 'q3'
            when month(date) in (9, 10, 11) then 'q4'
            end as fiscal_quarter,                   -- trimestre fiscal (puedes ajustarlo)
        week(date) as week,         -- semana del año
        dayofyear(date) as day_of_year  -- día del año
    
    from time_spine

)

select * from renamed

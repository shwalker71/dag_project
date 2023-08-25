with silver as (
    
    select 
        trading_date,
        close as close_silver,
        volume as volume_silver,
        open as open_silver,
        high as high_silver,
        low as low_silver
    from {{ source('gold_silver_data','dag_silver_snowflake_table')}}
)

select * from silver
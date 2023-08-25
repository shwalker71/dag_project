with gold as (
    
    select 
        trading_date,
        close as close_gold,
        volume as volume_gold,
        open as open_gold,
        high as high_gold,
        low as low_gold
    from {{ source('gold_silver_data','dag_gold_snowflake_table')}}
)

select * from gold
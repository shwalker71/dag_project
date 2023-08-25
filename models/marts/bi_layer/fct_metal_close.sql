with initial_metal_changes as (
    select
        trading_date,
        close_gold,
        close_silver,
        high_gold,
        low_gold,
        high_silver,
        low_silver,
        lead(close_gold) over 
        (order by trading_date desc) as prev_close_gold,
                lead(close_silver) over 
        (order by trading_date desc) as prev_close_silver
    from {{ref('int_metal_prices')}}
),
final_metal_changes as (
    select
        trading_date,
        close_gold,
        high_gold,
        low_gold,
        ((close_gold-prev_close_gold)
        /prev_close_gold) as pct_gold_change,
        close_silver,
        high_silver,
        low_silver,
        ((close_silver-prev_close_silver)
        /prev_close_silver) as pct_silver_change
    from initial_metal_changes
)
select * from final_metal_changes
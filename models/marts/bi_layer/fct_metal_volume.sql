with initial_metal_changes as (
    select
        trading_date,
        volume_gold,
        volume_silver,
        lead(volume_gold) over 
        (order by trading_date desc) as prev_volume_gold,
                lead(volume_silver) over 
        (order by trading_date desc) as prev_volume_silver
    from {{ref('int_metal_volume')}}
),
final_metal_changes as (
    select
        trading_date,
        volume_gold,
        volume_silver,
        ((volume_gold-prev_volume_gold)
        /prev_volume_gold) as pct_gold_change,
        ((volume_silver-prev_volume_silver)
        /prev_volume_silver) as pct_silver_change
    from initial_metal_changes
)
select * from final_metal_changes
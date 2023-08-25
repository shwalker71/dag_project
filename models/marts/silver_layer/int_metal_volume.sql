with gold as (
    select
        trading_date,
        volume_gold
    from {{ ref('stg_gold_data')}}
),
silver as (
    select
        trading_date,
        volume_silver
    from {{ ref('stg_silver_data')}}
),
combined_metals_volumes as (
    select
        g.trading_date,
        g.volume_gold,
        s.volume_silver
    from gold g
    left join silver s using (trading_date)
)
select * from combined_metals_volumes

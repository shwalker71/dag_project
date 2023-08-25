with gold as (
    select
        trading_date,
        close_gold,
        open_gold,
        high_gold,
        low_gold
    from {{ ref('stg_gold_data')}}
),
silver as (
    select
        trading_date,
        close_silver,
        open_silver,
        high_silver,
        low_silver
    from {{ ref('stg_silver_data')}}
),
combined_metals_prices as (
    select
        g.trading_date,
        g.close_gold,
        g.open_gold,
        g.high_gold,
        g.low_gold,
        s.close_silver,
        s.open_silver,
        s.high_silver,
        s.low_silver
    from gold g
    left join silver s using (trading_date)
)
select * from combined_metals_prices

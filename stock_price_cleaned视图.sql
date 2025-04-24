CREATE OR REPLACE VIEW public.stock_price_cleaned
AS SELECT day,
    code,
    volumn,
    zhenfu,
    change_rate as rate_close,
    round(((open - (close/(change_rate + 100)*100)) / (close/(change_rate + 100)*100) * 100::double precision)::numeric, 2) AS rate_open,
    round(((high - (close/(change_rate + 100)*100)) / (close/(change_rate + 100)*100) * 100::double precision)::numeric, 2) AS rate_high,
    round(((low - (close/(change_rate + 100)*100)) / (close/(change_rate + 100)*100) * 100::double precision)::numeric, 2) AS rate_low,
    turnover
   FROM stock_price t1
   ;
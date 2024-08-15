CREATE OR REPLACE VIEW bina.v_rates_deviation AS
SELECT
    r.price_date,
    r.symbol,
    r.price,
    ARRAY [
        ROUND((r.price - r.ema[1])::NUMERIC / r.ema[1]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[2])::NUMERIC / r.ema[2]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[3])::NUMERIC / r.ema[3]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[4])::NUMERIC / r.ema[4]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[5])::NUMERIC / r.ema[5]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[6])::NUMERIC / r.ema[6]::NUMERIC * 100, 2)] AS dev_pc,
    ARRAY [ROUND(r.ema[1]::NUMERIC, 6),
        ROUND(r.ema[2]::NUMERIC, 6),
        ROUND(r.ema[3]::NUMERIC, 6),
        ROUND(r.ema[4]::NUMERIC, 6),
        ROUND(r.ema[5]::NUMERIC, 6),
        ROUND(r.ema[6]::NUMERIC, 6)] AS ema
FROM bina.rates r
WHERE
    r.event_id <> 0
ORDER BY
    r.price_date DESC;

CREATE OR REPLACE VIEW bina.v_rates_deviation_all AS
SELECT
    r.price_date,
    r.symbol,
    r.price,
    ARRAY [
        ROUND((r.price - r.ema[1])::NUMERIC / r.ema[1]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[2])::NUMERIC / r.ema[2]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[3])::NUMERIC / r.ema[3]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[4])::NUMERIC / r.ema[4]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[5])::NUMERIC / r.ema[5]::NUMERIC * 100, 2),
        ROUND((r.price - r.ema[6])::NUMERIC / r.ema[6]::NUMERIC * 100, 2)] AS dev_pc,
    ARRAY [ROUND(r.ema[1]::NUMERIC, 6),
        ROUND(r.ema[2]::NUMERIC, 6),
        ROUND(r.ema[3]::NUMERIC, 6),
        ROUND(r.ema[4]::NUMERIC, 6),
        ROUND(r.ema[5]::NUMERIC, 6),
        ROUND(r.ema[6]::NUMERIC, 6)] AS ema
FROM bina.rates r;

drop VIEW if exists bina.v_rates_deviation_stat;
CREATE OR REPLACE VIEW bina.v_rates_deviation_stat AS
WITH
    RECURSIVE
    conf as (SELECT 3 as ema1_size, 6 as ema2_size, 12 as ema3_size, 24 as ema4_size, 72 as ema5_size),
    rates_ema AS (
        (SELECT r.symbol, r.price_date, r.avg_price as price, r.avg_price AS ema1, r.avg_price AS ema2, r.avg_price AS ema3, r.avg_price AS ema4, r.avg_price AS ema5
              FROM bina.rates_stat r
              WHERE (r.symbol, r.price_date) IN (SELECT r.symbol, min(r.price_date) as price_date
                        FROM bina.rates_stat r
                        GROUP BY r.symbol))
              UNION ALL
              SELECT r.symbol, r.price_date, r.avg_price as price,
                     (2::real / (s.ema1_size + 1)::real * l.price + (1 - 2::real / (s.ema1_size + 1)::real) * l.ema1)::REAL AS ema1,
                     (2::real / (s.ema2_size + 1)::real * l.price + (1 - 2::real / (s.ema2_size + 1)::real) * l.ema2)::REAL AS ema2,
                     (2::real / (s.ema3_size + 1)::real * l.price + (1 - 2::real / (s.ema3_size + 1)::real) * l.ema3)::REAL AS ema3,
                     (2::real / (s.ema4_size + 1)::real * l.price + (1 - 2::real / (s.ema4_size + 1)::real) * l.ema4)::REAL AS ema4,
                     (2::real / (s.ema5_size + 1)::real * l.price + (1 - 2::real / (s.ema5_size + 1)::real) * l.ema5)::REAL AS ema5
              FROM conf s, rates_ema l
                   JOIN bina.rates_stat r ON (r.price_date = l.price_date + INTERVAL '1 hour'
                                               and r.symbol = l.symbol)
              )
SELECT r.price_date,
       r.symbol,
       r.price,
       array[ROUND((r.price / r.ema1 * 100 - 100)::NUMERIC, 2),
           ROUND((r.price / r.ema2 * 100 - 100)::NUMERIC, 2),
           ROUND((r.price / r.ema3 * 100 - 100)::NUMERIC, 2),
           ROUND((r.price / r.ema4 * 100 - 100)::NUMERIC, 2),
           ROUND((r.price / r.ema5 * 100 - 100)::NUMERIC, 2)] as dev_pc,
       array[r.ema1, r.ema2, r.ema3, r.ema4, r.ema5] as ema
FROM rates_ema r
order by r.price_date;

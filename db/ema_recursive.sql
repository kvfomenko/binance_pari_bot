
WITH
    RECURSIVE
    conf as (SELECT 1 as ema1_size, 3 as ema2_size, 5 as ema3_size, 10 as ema4_size, 60 as ema5_size),
    rates_ema AS (
        (SELECT r.symbol, r.price_date, r.price, r.price AS ema1, r.price AS ema2, r.price AS ema3, r.price AS ema4, r.price AS ema5
              FROM bina.rates1s r
              WHERE r.symbol = 'BAKEUSDT'
                AND r.price_date = TO_TIMESTAMP('2023-10-08_16:09', 'YYYY-MM-DD HH24:MI')
              order by r.price_date LIMIT 1)
              UNION ALL
              SELECT r.symbol, r.price_date, r.price,
                     (2::real / (s.ema1_size + 1)::real * l.price + (1 - 2::real / (s.ema1_size + 1)::real) * l.ema1)::REAL AS ema1,
                     (2::real / (s.ema2_size + 1)::real * l.price + (1 - 2::real / (s.ema2_size + 1)::real) * l.ema2)::REAL AS ema2,
                     (2::real / (s.ema3_size + 1)::real * l.price + (1 - 2::real / (s.ema3_size + 1)::real) * l.ema3)::REAL AS ema3,
                     (2::real / (s.ema4_size + 1)::real * l.price + (1 - 2::real / (s.ema4_size + 1)::real) * l.ema4)::REAL AS ema4,
                     (2::real / (s.ema5_size + 1)::real * l.price + (1 - 2::real / (s.ema5_size + 1)::real) * l.ema5)::REAL AS ema5
              FROM conf s, rates_ema l
                   JOIN bina.rates1s r ON (r.price_date = l.price_date + INTERVAL '1 seconds'
                                               and r.symbol = l.symbol)
               where r.price_date <= TO_TIMESTAMP('2023-10-08_16:12', 'YYYY-MM-DD HH24:MI')
              )
SELECT TO_CHAR(r.price_date, 'MM/DD HH24:MI:SS') as price_date,
       REPLACE(r.price::varchar,'.',',') as price,
       REPLACE(r.ema1::varchar,'.',',') as ema1,
       REPLACE(r.ema2::varchar,'.',',') as ema2,
       REPLACE(r.ema3::varchar,'.',',') as ema3,
       REPLACE(r.ema4::varchar,'.',',') as ema4,
       REPLACE(r.ema5::varchar,'.',',') as ema5
FROM rates_ema r
order by r.price_date;




WITH
    RECURSIVE
    conf as (SELECT 1 as ema1_size, 3 as ema2_size, 5 as ema3_size, 10 as ema4_size, 60 as ema5_size),
    rates_ema AS (
        (SELECT r.symbol, r.price_date, r.price, r.price AS ema1, r.price AS ema2, r.price AS ema3, r.price AS ema4, r.price AS ema5
              FROM bina.rates1s r
              WHERE r.symbol = 'BAKEUSDT'
                AND r.price_date = TO_TIMESTAMP('2023-10-08_16:09', 'YYYY-MM-DD HH24:MI')
              order by r.price_date LIMIT 1)
              UNION ALL
              SELECT r.symbol, r.price_date, r.price,
                     (2::real / (s.ema1_size + 1)::real * l.price + (1 - 2::real / (s.ema1_size + 1)::real) * l.ema1)::REAL AS ema1,
                     (2::real / (s.ema2_size + 1)::real * l.price + (1 - 2::real / (s.ema2_size + 1)::real) * l.ema2)::REAL AS ema2,
                     (2::real / (s.ema3_size + 1)::real * l.price + (1 - 2::real / (s.ema3_size + 1)::real) * l.ema3)::REAL AS ema3,
                     (2::real / (s.ema4_size + 1)::real * l.price + (1 - 2::real / (s.ema3_size + 1)::real) * l.ema4)::REAL AS ema4,
                     (2::real / (s.ema5_size + 1)::real * l.price + (1 - 2::real / (s.ema5_size + 1)::real) * l.ema5)::REAL AS ema5
              FROM conf s, rates_ema l
                   JOIN bina.rates1s r ON (r.price_date = l.price_date + INTERVAL '1 seconds'
                                               and r.symbol = l.symbol)
               where r.price_date <= TO_TIMESTAMP('2023-10-08_16:12', 'YYYY-MM-DD HH24:MI')
              )
SELECT r.price_date,
       r.price,
       r.ema1,
       r.ema2,
       r.ema3,
       r.ema4,
       r.ema5
FROM rates_ema r
order by r.price_date;

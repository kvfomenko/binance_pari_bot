
WITH RECURSIVE xx AS (SELECT r.symbol, r.price_date,
                            r.min_price, r.max_price,
                            (1 + 2::real/100) * r.max_price AS stop_price,
                            (1 + 2::real/100) * r.max_price AS trailing_stop_price
                    FROM bina.rates1s r
                    WHERE r.symbol = 'STEEMUSDT'
                      AND r.price_date = '2024-02-18 00:00:05'
          UNION
          SELECT r.symbol, r.price_date,
                 r.min_price, r.max_price,
                 (1 + 2::real/100) * r.max_price AS stop_price,
                 least(xx.trailing_stop_price, (1 + 2::real/100) * r.max_price) as trailing_stop_price
          FROM xx, bina.rates1s r
          WHERE r.symbol = xx.symbol
              and r.price_date = xx.price_date + interval '1 second'
              and (r.price_date <= '2024-02-18 17:26:00'
                 or r.max_price >=
                     CASE WHEN r.max_price > xx.max_price then xx.trailing_stop_price -- prev_price
                     else (1 + 2::real/100) * r.max_price end
                   ))
SELECT xx.price_date, xx.min_price, xx.max_price,
       ROUND(xx.stop_price::numeric, 5) as stop_price,
       ROUND(xx.trailing_stop_price::numeric, 5) as trailing_stop_price,
       ROUND(((xx.trailing_stop_price - xx.max_price) / xx.trailing_stop_price * 100)::numeric, 2) as current_callback_pc,
       lpad('|', round((xx.max_price::numeric -0.23) * 1000, 0)::integer, ' ') as graph1,
       lpad('|', round((xx.trailing_stop_price::numeric -0.23) * 1000, 0)::integer, ' ') as graph1
FROM xx;

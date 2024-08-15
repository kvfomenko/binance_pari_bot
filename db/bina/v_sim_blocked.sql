CREATE OR REPLACE VIEW bina.v_sim_blocked AS
SELECT x.*
FROM (SELECT
          s.id AS subscriber_id,
          rs.symbol,
          ROUND((MAX(rs.max_price) / MIN(rs.min_price) * 100 - 100)::NUMERIC, 1) AS deviation,
          ROUND(MIN(rs.min_price)::NUMERIC, COALESCE(e.price_precision, 3)) AS min_price,
          ROUND(MAX(rs.max_price)::NUMERIC, COALESCE(e.price_precision, 3)) AS max_price,
          COALESCE(e.price_precision, 3) AS price_precision
      FROM bina.subscribers s
         , bina.rates_stat rs
           LEFT JOIN bina.exchange_info e ON (e.symbol = rs.symbol)
      WHERE
              rs.price_date > LOCALTIMESTAMP - ((s.symbol_config ->> 'block_symbol_period_hr') || ' hour')::INTERVAL
        AND   e.symbol = rs.symbol
        AND   e.quote_asset = 'USDT'
        AND   e.status = 'TRADING'
      GROUP BY
          s.id, s.symbol_config ->> 'block_symbol_dev_pc',
          rs.symbol, e.price_precision
      HAVING
                          MAX(rs.max_price) / MIN(rs.min_price) * 100 - 100 >
                          (s.symbol_config ->> 'block_symbol_dev_pc')::NUMERIC) x;

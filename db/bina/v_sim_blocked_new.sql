CREATE OR REPLACE VIEW bina.v_sim_blocked_new AS
SELECT x.*
FROM (SELECT
          s.id AS subscriber_id,
          rs.symbol,
          (SELECT TO_CHAR(MIN(s1.price_date), 'MM/DD HH24:MI')
           FROM bina.rates_stat s1
           WHERE
               s1.symbol = rs.symbol) AS symbol_start_date
      FROM bina.subscribers s
         , bina.rates_stat rs
           LEFT JOIN bina.exchange_info e ON (e.symbol = rs.symbol)
      WHERE
            rs.price_date >= DATE_TRUNC('hour', LOCALTIMESTAMP) - INTERVAL '1 hour'
        AND e.symbol = rs.symbol
        AND e.quote_asset = 'USDT'
        AND e.status = 'TRADING'
        AND NOT EXISTS (SELECT 1
                        FROM bina.rates_stat s0
                        WHERE
                              s0.symbol = rs.symbol
                          AND s0.price_date IN (
                                                DATE_TRUNC('hour', LOCALTIMESTAMP -
                                                                   ((s.symbol_config ->> 'block_new_symbol_hr') || ' hour')::INTERVAL),
-- additional check: if this symbol was week before or not
                                                DATE_TRUNC('hour', LOCALTIMESTAMP
                                                    -
                                                                   ((s.symbol_config ->> 'block_new_symbol_hr') || ' hour')::INTERVAL)
                                                    - INTERVAL '7 days'))
      GROUP BY
          s.id, s.symbol_config ->> 'block_new_symbol_hr', rs.symbol) x;

create view bina.v_sim_blocked (subscriber_id, symbol, deviation, min_price, max_price, price_precision) as
SELECT
    subscriber_id,
    symbol,
    deviation,
    min_price,
    max_price,
    price_precision
FROM (SELECT
          s.id AS subscriber_id,
          rs.symbol,
          ROUND((MAX(rs.max_price) / MIN(rs.min_price) * 100::DOUBLE PRECISION - 100::DOUBLE PRECISION)::NUMERIC,
                1) AS deviation,
          ROUND(MIN(rs.min_price)::NUMERIC, COALESCE(e.price_precision::INTEGER, 3)) AS min_price,
          ROUND(MAX(rs.max_price)::NUMERIC, COALESCE(e.price_precision::INTEGER, 3)) AS max_price,
          COALESCE(e.price_precision::INTEGER, 3) AS price_precision
      FROM subscribers s
         , rates_stat rs
           LEFT JOIN exchange_info e ON e.symbol::TEXT = rs.symbol::TEXT
      WHERE
              rs.price_date >
              (LOCALTIMESTAMP - (((s.symbol_config ->> 'block_symbol_period_hr'::TEXT) || ' hour'::TEXT)::INTERVAL))
        AND   e.symbol::TEXT = rs.symbol::TEXT
        AND   e.quote_asset::TEXT = 'USDT'::TEXT
        AND   e.status::TEXT = 'TRADING'::TEXT
      GROUP BY s.id, (s.symbol_config ->> 'block_symbol_dev_pc'::TEXT), rs.symbol, e.price_precision
      HAVING
              (MAX(rs.max_price) / MIN(rs.min_price) * 100::DOUBLE PRECISION - 100::DOUBLE PRECISION) >
              ((s.symbol_config ->> 'block_symbol_dev_pc'::TEXT)::NUMERIC)::DOUBLE PRECISION) x;

alter table bina.v_sim_blocked
    owner to bina;

create view bina.v_rates_deviation(price_date, symbol, price, dev_pc, ema) as
SELECT
    price_date,
    symbol,
    price,
    ARRAY [ROUND((price - ema[1])::NUMERIC / ema[1]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[2])::NUMERIC / ema[2]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[3])::NUMERIC / ema[3]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[4])::NUMERIC / ema[4]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[5])::NUMERIC / ema[5]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[6])::NUMERIC / ema[6]::NUMERIC * 100::NUMERIC, 2)] AS dev_pc,
    ARRAY [ROUND(ema[1]::NUMERIC, 6), ROUND(ema[2]::NUMERIC, 6), ROUND(ema[3]::NUMERIC, 6), ROUND(ema[4]::NUMERIC, 6), ROUND(ema[5]::NUMERIC, 6), ROUND(ema[6]::NUMERIC, 6)] AS ema
FROM rates r
WHERE
    event_id <> 0
ORDER BY price_date DESC;

alter table bina.v_rates_deviation
    owner to bina;

create view bina.v_sim_blocked_new(subscriber_id, symbol, symbol_start_date) as
SELECT
    subscriber_id,
    symbol,
    symbol_start_date
FROM (SELECT
          s.id AS subscriber_id,
          rs.symbol,
          (SELECT TO_CHAR(MIN(s1.price_date), 'MM/DD HH24:MI'::TEXT) AS to_char
           FROM rates_stat s1
           WHERE
               s1.symbol::TEXT = rs.symbol::TEXT) AS symbol_start_date
      FROM subscribers s
         , rates_stat rs
           LEFT JOIN exchange_info e ON e.symbol::TEXT = rs.symbol::TEXT
      WHERE
              rs.price_date >= (DATE_TRUNC('hour'::TEXT, LOCALTIMESTAMP) - '01:00:00'::INTERVAL)
        AND   e.symbol::TEXT = rs.symbol::TEXT
        AND   e.quote_asset::TEXT = 'USDT'::TEXT
        AND   e.status::TEXT = 'TRADING'::TEXT
        AND   NOT (EXISTS (SELECT 1
                           FROM rates_stat s0
                           WHERE
                                 s0.symbol::TEXT = rs.symbol::TEXT
                             AND (s0.price_date = ANY (ARRAY [DATE_TRUNC('hour'::TEXT, LOCALTIMESTAMP -
                                                                                       (((s.symbol_config ->> 'block_new_symbol_hr'::TEXT) || ' hour'::TEXT)::INTERVAL)),
                                   DATE_TRUNC('hour'::TEXT, LOCALTIMESTAMP -
                                                            (((s.symbol_config ->> 'block_new_symbol_hr'::TEXT) || ' hour'::TEXT)::INTERVAL)) -
                                   '7 days'::INTERVAL]))))
      GROUP BY s.id, (s.symbol_config ->> 'block_new_symbol_hr'::TEXT), rs.symbol) x;

alter table bina.v_sim_blocked_new
    owner to bina;

create view bina.v_tables (schema, table_name, oid, table_size, table_total_size, vacuum_pc, table_ts, reloptions) as
SELECT
    ns.nspname AS schema,
    t.relname AS table_name,
    t.oid,
    PG_RELATION_SIZE(t.oid::REGCLASS) AS table_size,
    PG_TOTAL_RELATION_SIZE(t.oid::REGCLASS) AS table_total_size,
    ROUND((PG_TOTAL_RELATION_SIZE(t.oid::REGCLASS) - PG_RELATION_SIZE(t.oid::REGCLASS))::NUMERIC /
          NULLIF(PG_TOTAL_RELATION_SIZE(t.oid::REGCLASS), 0)::NUMERIC * 100::NUMERIC, 0) AS vacuum_pc,
    ts.spcname AS table_ts,
    t.reloptions
FROM pg_class t
     JOIN      pg_namespace ns ON ns.oid = t.relnamespace
     LEFT JOIN pg_tablespace ts ON ts.oid = "substring"(PG_RELATION_FILEPATH(t.oid::REGCLASS), 'pg_tblspc/(.+?)/'::TEXT)::INTEGER::OID
WHERE
      t.relkind = 'r'::"char"
  AND (ns.nspname <> ALL (ARRAY ['pg_catalog'::NAME, 'information_schema'::NAME, 'public'::NAME, 'pg_toast'::NAME]))
GROUP BY ns.nspname, t.relname, t.oid, ts.spcname, t.reloptions
ORDER BY ns.nspname, t.relname, t.oid;

alter table bina.v_tables
    owner to bina;

create view bina.v_all_profit
            (subscriber_id, start_trading_date, profit_date, symbol, trading_side, order_type, sub_type, filled_usdt,
             position_usdt, commission_usdt, profit_pc)
as
SELECT
    subscriber_id,
    start_trading_date,
    profit_date,
    symbol,
    CASE
        WHEN side = 'SELL'::TEXT THEN 'BUY'::TEXT
        ELSE 'SELL'::TEXT
    END AS trading_side,
    order_type,
    CASE
        WHEN (order_type = ANY (ARRAY ['take_profit'::TEXT, 'stop_loss'::TEXT])) AND
             POSITION(('.'::TEXT) IN ("substring"(coid, POSITION(('.'::TEXT) IN (coid)) + 1))) > 0 THEN "substring"(
                "substring"(coid, POSITION(('.'::TEXT) IN (coid)) + 1), 1,
                POSITION(('.'::TEXT) IN ("substring"(coid, POSITION(('.'::TEXT) IN (coid)) + 1))) - 1)
        ELSE NULL::TEXT
    END AS sub_type,
    ROUND(filled_usdt + COALESCE((partially_filled ->> 'profit'::TEXT)::NUMERIC, 0::NUMERIC), 3) AS filled_usdt,
    ROUND(position_usdt, 2) AS position_usdt,
    ROUND(commission_usdt + COALESCE((partially_filled ->> 'commission'::TEXT)::NUMERIC, 0::NUMERIC),
          4) AS commission_usdt,
    ROUND((filled_usdt + COALESCE((partially_filled ->> 'profit'::TEXT)::NUMERIC, 0::NUMERIC)) / position_usdt *
          100::NUMERIC, 1) AS profit_pc
FROM (SELECT
          b.subscriber_id,
          b.profit_date,
          b.symbol,
          b.side,
          b.order_type,
          b.order_id,
          b.coid,
          b.filled_usdt,
          b.commission_usdt,
          b.start_trading_date,
          b.position_usdt,
          (SELECT
               JSON_BUILD_OBJECT('profit', SUM((l2.params ->> 'realisedProfit'::TEXT)::NUMERIC), 'commission',
                                 SUM((l2.params ->> 'commissionAmount'::TEXT)::NUMERIC)) AS json_build_object
           FROM trade_log l2
           WHERE
                 l2.subscriber_id = b.subscriber_id
             AND l2.log_date >= b.start_trading_date
             AND l2.log_date <= b.profit_date
             AND (l2.comment::TEXT = ANY
                  (ARRAY ['order trade update'::CHARACTER VARYING, 'order outside bot-trading update'::CHARACTER VARYING]::TEXT[]))
             AND (l2.params ->> 'symbol'::TEXT) = b.symbol
             AND ((l2.params ->> 'orderStatus'::TEXT) = 'PARTIALLY_FILLED'::TEXT AND
                  (l2.params ->> 'orderId'::TEXT) = b.order_id OR
                  (l2.params ->> 'orderStatus'::TEXT) = 'FILLED'::TEXT AND
                  (l2.params ->> 'orderId'::TEXT) <> b.order_id AND ((l2.params ->> 'clientOrderId'::TEXT) ~~ ANY
                                                                     (ARRAY ['initial.%'::TEXT, 'average.%'::TEXT, 'third.%'::TEXT])))) AS partially_filled
      FROM (SELECT
                l.subscriber_id,
                l.log_date AS profit_date,
                l.params ->> 'symbol'::TEXT AS symbol,
                l.params ->> 'orderSide'::TEXT AS side,
                l.params ->> 'order_type'::TEXT AS order_type,
                l.params ->> 'orderId'::TEXT AS order_id,
                l.params ->> 'clientOrderId'::TEXT AS coid,
                (l.params ->> 'realisedProfit'::TEXT)::NUMERIC AS filled_usdt,
                (l.params ->> 'commissionAmount'::TEXT)::NUMERIC AS commission_usdt,
                COALESCE((SELECT MAX(l2.log_date) AS max
                          FROM trade_log l2
                          WHERE
                                l2.subscriber_id = l.subscriber_id
                            AND l2.comment::TEXT = 'start trading'::TEXT
                            AND l2.log_date >= (l.log_date - '7 days'::INTERVAL)
                            AND l2.log_date <= l.log_date
                            AND (l2.params ->> 'symbol'::TEXT) = (l.params ->> 'symbol'::TEXT)),
                         l.log_date - '00:01:00'::INTERVAL) AS start_trading_date,
                ((l.params ->> 'originalQuantity'::TEXT)::NUMERIC) *
                ((l.params ->> 'averagePrice'::TEXT)::NUMERIC) AS position_usdt
            FROM trade_log l
               , subscribers s
            WHERE
                  s.id = l.subscriber_id
              AND (l.comment::TEXT = ANY
                   (ARRAY ['order trade update'::CHARACTER VARYING, 'order outside bot-trading update'::CHARACTER VARYING]::TEXT[]))
              AND (l.params ->> 'orderStatus'::TEXT) = 'FILLED'::TEXT) b) a
WHERE
        order_type <> ALL (ARRAY ['initial'::TEXT, 'average'::TEXT, 'third'::TEXT])
ORDER BY profit_date DESC;

alter table bina.v_all_profit
    owner to bina;

create view bina.v_total_profit2
            (acc, subscriber_id, total_count, positive_count, negative_count, avg_profit_pc, sum_profit_pc,
             sum_profit_usdt, min_date)
as
SELECT
    (s.name::TEXT || ' n'::TEXT) || s.acc_num AS acc,
    x.subscriber_id,
    x.total_count,
    x.positive_count,
    x.negative_count,
    x.avg_profit_pc,
    x.sum_profit_pc,
    x.sum_profit_usdt,
    x.min_date
FROM (SELECT
          p.subscriber_id,
          COUNT(1) AS total_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc > 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS positive_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc < 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS negative_count,
          ROUND(AVG(p.profit_pc), 1) AS avg_profit_pc,
          SUM(p.profit_pc) AS sum_profit_pc,
          SUM(p.filled_usdt) AS sum_profit_usdt,
          TO_CHAR(MIN(p.profit_date), 'YYYY-MM-DD'::TEXT) AS min_date
      FROM v_all_profit p
      WHERE
          p.profit_date > '2023-12-06 00:00:00'::TIMESTAMP WITHOUT TIME ZONE
      GROUP BY p.subscriber_id) x
   , subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY ((s.name::TEXT || ' n'::TEXT) || s.acc_num);

alter table bina.v_total_profit2
    owner to bina;

create view bina.v_total_profit
            (acc, subscriber_id, total_count, positive_count, negative_count, avg_profit_pc, sum_profit_pc,
             sum_profit_usdt, sum_position_usdt, min_date)
as
SELECT
    (s.name::TEXT || ' n'::TEXT) || s.acc_num AS acc,
    x.subscriber_id,
    x.total_count,
    x.positive_count,
    x.negative_count,
    x.avg_profit_pc,
    x.sum_profit_pc,
    x.sum_profit_usdt,
    x.sum_position_usdt,
    x.min_date
FROM (SELECT
          p.subscriber_id,
          COUNT(1) AS total_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc > 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS positive_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc < 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS negative_count,
          ROUND(AVG(p.profit_pc), 1) AS avg_profit_pc,
          SUM(p.profit_pc) AS sum_profit_pc,
          SUM(p.filled_usdt) AS sum_profit_usdt,
          SUM(p.position_usdt) AS sum_position_usdt,
          TO_CHAR(MIN(p.profit_date), 'YYYY-MM-DD'::TEXT) AS min_date
      FROM v_all_profit p
      GROUP BY p.subscriber_id) x
   , subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY ((s.name::TEXT || ' n'::TEXT) || s.acc_num);

alter table bina.v_total_profit
    owner to bina;

create view bina.v_profit_by_day
            (acc, profit_date, subscriber_id, total_count, positive_count, negative_count, avg_profit_pc, sum_profit_pc,
             sum_profit_usdt, sum_position_usdt)
as
SELECT
    (s.name::TEXT || ' n'::TEXT) || s.acc_num AS acc,
    x.profit_date,
    x.subscriber_id,
    x.total_count,
    x.positive_count,
    x.negative_count,
    x.avg_profit_pc,
    x.sum_profit_pc,
    x.sum_profit_usdt,
    x.sum_position_usdt
FROM (SELECT
          p.profit_date::DATE AS profit_date,
          p.subscriber_id,
          COUNT(
                  CASE
                      WHEN p.profit_pc <> 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS total_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc > 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS positive_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc < 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS negative_count,
          ROUND(AVG(p.profit_pc), 1) AS avg_profit_pc,
          SUM(p.profit_pc) AS sum_profit_pc,
          SUM(p.filled_usdt) AS sum_profit_usdt,
          SUM(p.position_usdt) AS sum_position_usdt
      FROM v_all_profit p
      GROUP BY (p.profit_date::DATE), p.subscriber_id) x
   , subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY x.profit_date DESC, ((s.name::TEXT || ' n'::TEXT) || s.acc_num);

alter table bina.v_profit_by_day
    owner to bina;

create view bina.v_profit_by_month
            (acc, profit_date, subscriber_id, total_count, positive_count, negative_count, avg_profit_pc, sum_profit_pc,
             sum_profit_usdt, sum_position_usdt)
as
SELECT
    (s.name::TEXT || ' n'::TEXT) || s.acc_num AS acc,
    x.profit_date,
    x.subscriber_id,
    x.total_count,
    x.positive_count,
    x.negative_count,
    x.avg_profit_pc,
    x.sum_profit_pc,
    x.sum_profit_usdt,
    x.sum_position_usdt
FROM (SELECT
          DATE_TRUNC('month'::TEXT, p.profit_date) AS profit_date,
          p.subscriber_id,
          COUNT(1) AS total_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc > 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS positive_count,
          COUNT(
                  CASE
                      WHEN p.profit_pc < 0::NUMERIC THEN 1
                      ELSE NULL::INTEGER
                  END) AS negative_count,
          ROUND(AVG(p.profit_pc), 1) AS avg_profit_pc,
          SUM(p.profit_pc) AS sum_profit_pc,
          SUM(p.filled_usdt) AS sum_profit_usdt,
          SUM(p.position_usdt) AS sum_position_usdt
      FROM v_all_profit p
      GROUP BY (DATE_TRUNC('month'::TEXT, p.profit_date)), p.subscriber_id) x
   , subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY x.profit_date DESC, ((s.name::TEXT || ' n'::TEXT) || s.acc_num);

alter table bina.v_profit_by_month
    owner to bina;

create view bina.v_rates_deviation_all(price_date, symbol, price, dev_pc, ema) as
SELECT
    price_date,
    symbol,
    price,
    ARRAY [ROUND((price - ema[1])::NUMERIC / ema[1]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[2])::NUMERIC / ema[2]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[3])::NUMERIC / ema[3]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[4])::NUMERIC / ema[4]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[5])::NUMERIC / ema[5]::NUMERIC * 100::NUMERIC, 2), ROUND(
                    (price - ema[6])::NUMERIC / ema[6]::NUMERIC * 100::NUMERIC, 2)] AS dev_pc,
    ARRAY [ROUND(ema[1]::NUMERIC, 6), ROUND(ema[2]::NUMERIC, 6), ROUND(ema[3]::NUMERIC, 6), ROUND(ema[4]::NUMERIC, 6), ROUND(ema[5]::NUMERIC, 6), ROUND(ema[6]::NUMERIC, 6)] AS ema
FROM rates r;

alter table bina.v_rates_deviation_all
    owner to bina;

create view bina.v_rates_deviation_stat(price_date, symbol, price, dev_pc, ema) as
WITH
    RECURSIVE
    conf      AS (SELECT
                      3 AS ema1_size,
                      6 AS ema2_size,
                      12 AS ema3_size,
                      24 AS ema4_size,
                      72 AS ema5_size),
    rates_ema AS (SELECT
                      r_1.symbol,
                      r_1.price_date,
                      r_1.avg_price AS price,
                      r_1.avg_price AS ema1,
                      r_1.avg_price AS ema2,
                      r_1.avg_price AS ema3,
                      r_1.avg_price AS ema4,
                      r_1.avg_price AS ema5
                  FROM rates_stat r_1
                  WHERE
                      ((r_1.symbol::TEXT, r_1.price_date) IN (SELECT
                                                                  r_2.symbol,
                                                                  MIN(r_2.price_date) AS price_date
                                                              FROM rates_stat r_2
                                                              GROUP BY r_2.symbol))
                  UNION ALL
                  SELECT
                      r_1.symbol,
                      r_1.price_date,
                      r_1.avg_price AS price,
                      (2::REAL / (s.ema1_size + 1)::REAL * l.price +
                       (1::DOUBLE PRECISION - 2::REAL / (s.ema1_size + 1)::REAL) * l.ema1)::REAL AS ema1,
                      (2::REAL / (s.ema2_size + 1)::REAL * l.price +
                       (1::DOUBLE PRECISION - 2::REAL / (s.ema2_size + 1)::REAL) * l.ema2)::REAL AS ema2,
                      (2::REAL / (s.ema3_size + 1)::REAL * l.price +
                       (1::DOUBLE PRECISION - 2::REAL / (s.ema3_size + 1)::REAL) * l.ema3)::REAL AS ema3,
                      (2::REAL / (s.ema4_size + 1)::REAL * l.price +
                       (1::DOUBLE PRECISION - 2::REAL / (s.ema4_size + 1)::REAL) * l.ema4)::REAL AS ema4,
                      (2::REAL / (s.ema5_size + 1)::REAL * l.price +
                       (1::DOUBLE PRECISION - 2::REAL / (s.ema5_size + 1)::REAL) * l.ema5)::REAL AS ema5
                  FROM conf s
                     , rates_ema l
                       JOIN rates_stat r_1 ON r_1.price_date = (l.price_date + '01:00:00'::INTERVAL) AND
                                              r_1.symbol::TEXT = l.symbol::TEXT)
SELECT
    price_date,
    symbol,
    price,
    ARRAY [ROUND((price / ema1 * 100::DOUBLE PRECISION - 100::DOUBLE PRECISION)::NUMERIC, 2), ROUND(
            (price / ema2 * 100::DOUBLE PRECISION - 100::DOUBLE PRECISION)::NUMERIC, 2), ROUND(
            (price / ema3 * 100::DOUBLE PRECISION - 100::DOUBLE PRECISION)::NUMERIC, 2), ROUND(
            (price / ema4 * 100::DOUBLE PRECISION - 100::DOUBLE PRECISION)::NUMERIC, 2), ROUND(
            (price / ema5 * 100::DOUBLE PRECISION - 100::DOUBLE PRECISION)::NUMERIC, 2)] AS dev_pc,
    ARRAY [ema1, ema2, ema3, ema4, ema5] AS ema
FROM rates_ema r
ORDER BY price_date;

alter table bina.v_rates_deviation_stat
    owner to bina;



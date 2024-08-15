DROP VIEW IF EXISTS bina.v_total_profit2;
DROP VIEW IF EXISTS bina.v_total_profit;
DROP VIEW IF EXISTS bina.v_profit_by_day;
DROP VIEW IF EXISTS bina.v_profit_by_month;
DROP VIEW IF EXISTS bina.v_all_profit;

CREATE OR REPLACE VIEW bina.v_all_profit AS
SELECT
    a.subscriber_id,
    --a.acc_num,
    a.start_trading_date,
    a.profit_date,
    a.symbol,
    CASE WHEN a.side = 'SELL' THEN 'BUY' ELSE 'SELL' END trading_side,
    a.order_type,
    CASE
        WHEN a.order_type IN ('take_profit', 'stop_loss') AND
             POSITION('.' IN SUBSTRING(a.coid, POSITION('.' IN a.coid) + 1)) > 0
            THEN
            SUBSTRING(SUBSTRING(a.coid, POSITION('.' IN a.coid) + 1),
                      1, POSITION('.' IN SUBSTRING(a.coid, POSITION('.' IN a.coid) + 1)) - 1
            )
    END AS sub_type,
    ROUND(a.filled_usdt + COALESCE((a.partially_filled ->> 'profit')::NUMERIC, 0), 3) AS filled_usdt,
    ROUND(a.position_usdt, 2) AS position_usdt,
    ROUND(a.commission_usdt + COALESCE((a.partially_filled ->> 'commission')::NUMERIC, 0), 4) AS commission_usdt,
    ROUND((a.filled_usdt + COALESCE((a.partially_filled ->> 'profit')::NUMERIC, 0)) / a.position_usdt * 100,
          1) AS profit_pc
FROM (SELECT b.*,
        (SELECT
               JSON_BUILD_OBJECT(
                       'profit', SUM((l2.params ->> 'realisedProfit')::NUMERIC),
                       'commission', SUM((l2.params ->> 'commissionAmount')::NUMERIC))
           FROM bina.trade_log l2
           WHERE
                 l2.subscriber_id = b.subscriber_id
             AND l2.log_date BETWEEN b.start_trading_date AND b.profit_date
             AND l2.comment IN ('order trade update', 'order outside bot-trading update')
             AND l2.params ->> 'symbol' = b.symbol
             AND (l2.params ->> 'orderStatus' = 'PARTIALLY_FILLED' AND l2.params ->> 'orderId' = b.order_id
                 OR l2.params ->> 'orderStatus' = 'FILLED' AND l2.params ->> 'orderId' != b.order_id
                    AND l2.params ->> 'clientOrderId' LIKE ANY (array['initial.%','average.%','third.%']) )
           ) AS partially_filled
    from (SELECT
          l.subscriber_id,
          --s.acc_num,
          l.log_date AS profit_date,
          l.params ->> 'symbol' AS symbol,
          l.params ->> 'orderSide' AS side,
          l.params ->> 'order_type' AS order_type,
          l.params ->> 'orderId' as order_id,
          l.params ->> 'clientOrderId' AS coid,
          (l.params ->> 'realisedProfit')::NUMERIC AS filled_usdt,
          (l.params ->> 'commissionAmount')::NUMERIC AS commission_usdt,
          COALESCE((SELECT MAX(l2.log_date)
                             FROM bina.trade_log l2
                             WHERE
                                   l2.subscriber_id = l.subscriber_id
                               AND l2.comment = 'start trading'
                               AND l2.log_date BETWEEN l.log_date - INTERVAL '7 day' AND l.log_date
                               AND l2.params ->> 'symbol' = l.params ->> 'symbol'),
                            l.log_date - INTERVAL '1 minute') AS start_trading_date,
          (l.params ->> 'originalQuantity')::NUMERIC *
          (l.params ->> 'averagePrice')::NUMERIC AS position_usdt
      FROM bina.trade_log l
         , bina.subscribers s
      WHERE
            s.id = l.subscriber_id
            --AND l.log_date >= LOCALTIMESTAMP - INTERVAL '14 days'
        AND l.comment IN ('order trade update', 'order outside bot-trading update')
        AND l.params ->> 'orderStatus' = 'FILLED'
      ) b ) a
WHERE a.order_type NOT IN ('initial','average','third')
  -- AND a.filled_usdt != 0
  --AND a.position_usdt != 0
ORDER BY a.profit_date DESC;

CREATE OR REPLACE VIEW bina.v_total_profit2 AS
SELECT s.name || ' n' || s.acc_num AS acc, x.*
FROM (SELECT
          subscriber_id,
          COUNT(1) AS total_count,
          COUNT(CASE WHEN profit_pc > 0 THEN 1 END) AS positive_count,
          COUNT(CASE WHEN profit_pc < 0 THEN 1 END) AS negative_count,
          ROUND(AVG(profit_pc), 1) AS avg_profit_pc,
          SUM(profit_pc) AS sum_profit_pc,
          SUM(filled_usdt) AS sum_profit_usdt,
          TO_CHAR(MIN(profit_date), 'YYYY-MM-DD') AS min_date
      FROM bina.v_all_profit p
      WHERE
          p.profit_date > '2023-12-06'
      GROUP BY p.subscriber_id) x
   , bina.subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY acc;

CREATE OR REPLACE VIEW bina.v_total_profit AS
SELECT s.name || ' n' || s.acc_num AS acc, x.*
FROM (SELECT
          subscriber_id,
          COUNT(1) AS total_count,
          COUNT(CASE WHEN profit_pc > 0 THEN 1 END) AS positive_count,
          COUNT(CASE WHEN profit_pc < 0 THEN 1 END) AS negative_count,
          ROUND(AVG(profit_pc), 1) AS avg_profit_pc,
          SUM(profit_pc) AS sum_profit_pc,
          SUM(filled_usdt) AS sum_profit_usdt,
          SUM(position_usdt) as sum_position_usdt,
          TO_CHAR(MIN(profit_date), 'YYYY-MM-DD') AS min_date
      FROM bina.v_all_profit p
      GROUP BY p.subscriber_id) x
   , bina.subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY acc;

CREATE OR REPLACE VIEW bina.v_profit_by_day AS
SELECT s.name || ' n' || s.acc_num AS acc, x.*
FROM (SELECT
          p.profit_date::DATE,
          p.subscriber_id,
          COUNT(CASE WHEN p.profit_pc != 0 THEN 1 END) AS total_count,
          COUNT(CASE WHEN p.profit_pc > 0 THEN 1 END) AS positive_count,
          COUNT(CASE WHEN p.profit_pc < 0 THEN 1 END) AS negative_count,
          ROUND(AVG(p.profit_pc), 1) AS avg_profit_pc,
          SUM(p.profit_pc) AS sum_profit_pc,
          SUM(p.filled_usdt) AS sum_profit_usdt,
          SUM(p.position_usdt) as sum_position_usdt
      FROM bina.v_all_profit p
      GROUP BY p.profit_date::DATE, p.subscriber_id) x
   , bina.subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY x.profit_date DESC, acc;

CREATE OR REPLACE VIEW bina.v_profit_by_month AS
SELECT s.name || ' n' || s.acc_num AS acc, x.*
FROM (SELECT
          date_trunc('month', p.profit_date) as profit_date,
          p.subscriber_id,
          COUNT(1) AS total_count,
          COUNT(CASE WHEN p.profit_pc > 0 THEN 1 END) AS positive_count,
          COUNT(CASE WHEN p.profit_pc < 0 THEN 1 END) AS negative_count,
          ROUND(AVG(p.profit_pc), 1) AS avg_profit_pc,
          SUM(p.profit_pc) AS sum_profit_pc,
          SUM(p.filled_usdt) AS sum_profit_usdt,
          SUM(p.position_usdt) as sum_position_usdt
      FROM bina.v_all_profit p
      GROUP BY date_trunc('month', p.profit_date), p.subscriber_id) x
   , bina.subscribers s
WHERE
    s.id = x.subscriber_id
ORDER BY x.profit_date DESC, acc;

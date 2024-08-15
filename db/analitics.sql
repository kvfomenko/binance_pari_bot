

WITH
t1 AS (SELECT s.id,
       s.binance_leverage,
       (s.config -> 'short' ->> 'new_short_order_balance_pc')::NUMERIC AS new_short_order_balance_pc,
       (s.config -> 'short' ->> 'avg_short_order_balance_pc')::NUMERIC AS avg_short_order_balance_pc,
       (s.config -> 'short' ->> 'third_short_order_balance_pc')::NUMERIC AS third_short_order_balance_pc,
       (s.config -> 'short' ->> 'leverage_short')::NUMERIC AS leverage_short,
       (s.config -> 'short' ->> 'avg_dev_short_pc')::NUMERIC AS avg_dev_short_pc,
       CASE
           WHEN (s.config -> 'short' ->> 'third_short_order_balance_pc')::NUMERIC > 0
               THEN (s.config -> 'short' ->> 'third_dev_short_pc')::NUMERIC
           ELSE 0
       END AS third_dev_short_pc,
       (s.config -> 'short' ->> 'stop_loss_short_pc')::NUMERIC AS stop_loss_short_pc,

       ROUND((s.config -> 'short' ->> 'new_short_order_balance_pc')::NUMERIC
                 * (s.config -> 'short' ->> 'leverage_short')::NUMERIC
                 /*/ s.binance_leverage*/, 2) AS initial_short_balance_pc,

       - ROUND((s.config -> 'short' ->> 'new_short_order_balance_pc')::NUMERIC
                   * (s.config -> 'short' ->> 'leverage_short')::NUMERIC
                   /*/ s.binance_leverage*/
                   * (s.config -> 'short' ->> 'avg_dev_short_pc')::NUMERIC
                   / 100, 2) AS unrealized_profit_before_avg_pc
   FROM bina.subscribers s
   WHERE
         (s.config -> 'short' ->> 'avg_short_order_balance_pc')::NUMERIC > 0
     /*AND s.id = ln_subscriber_id*/),
t2 AS (SELECT
       s.*,
       --s.avg_dev_short_pc + s.third_dev_short_pc as abs_third_dev_short_pc,
       ROUND(
                           (100 - s.new_short_order_balance_pc)
                           * s.avg_short_order_balance_pc * s.leverage_short /*/ s.binance_leverage*/ / 100
                   * (1 + s.avg_dev_short_pc / 100) /*short price correction*/
           , 2) AS average_short_balance_pc
   FROM t1 s),
t3 AS (SELECT
       s.*,
       s.initial_short_balance_pc + s.average_short_balance_pc -
       s.unrealized_profit_before_avg_pc AS after_average_short_balance_pc,
       -ROUND(s.average_short_balance_pc / (s.initial_short_balance_pc + s.average_short_balance_pc) *
              s.avg_dev_short_pc, 2) AS position_price_after_avg_pc,
       CASE
           WHEN s.third_dev_short_pc = 0 THEN 0
           ELSE ROUND(s.avg_dev_short_pc -
                      s.average_short_balance_pc / (s.initial_short_balance_pc + s.average_short_balance_pc) *
                      s.avg_dev_short_pc
                          + s.third_dev_short_pc, 2)
       END AS avg_to_third_dev_short_pc
   FROM t2 s),
t4 AS (SELECT
       s.*,
       ROUND(- s.after_average_short_balance_pc * s.avg_to_third_dev_short_pc / 100,
             2) AS unrealized_profit_before_third_pc,
       ROUND(
                           (100 - s.new_short_order_balance_pc - s.average_short_balance_pc)
                           * s.third_short_order_balance_pc * s.leverage_short /*/ s.binance_leverage*/ / 100
                   * (1 + s.avg_to_third_dev_short_pc / 100) /*short price correction*/
           , 2) AS third_short_balance_pc
   FROM t3 s),
t5 AS (SELECT
       s.*,
       ROUND(s.initial_short_balance_pc + s.average_short_balance_pc + s.third_short_balance_pc
                 - s.unrealized_profit_before_avg_pc - s.unrealized_profit_before_third_pc,
             2) AS after_third_short_balance_pc,
       ROUND(s.position_price_after_avg_pc
                 - s.third_short_balance_pc /
                   (s.initial_short_balance_pc + s.average_short_balance_pc + s.third_short_balance_pc)
                 * s.avg_to_third_dev_short_pc, 2) AS position_price_after_third_pc
   FROM t4 s),
t6 AS (SELECT
       s.*,
       s.position_price_after_third_pc - s.stop_loss_short_pc AS position_price_before_stoploss_pc,
       ROUND(case when s.unrealized_profit_before_third_pc = 0
           then s.unrealized_profit_before_avg_pc
           else s.unrealized_profit_before_third_pc end
                 - s.after_third_short_balance_pc * s.stop_loss_short_pc / 100,
             2) AS unrealized_profit_before_stoploss_pc
   FROM t5 s)
SELECT
    s.*,
    s.after_third_short_balance_pc - s.unrealized_profit_before_stoploss_pc AS total_used_balance_pc,
    CASE
        WHEN s.after_third_short_balance_pc - s.unrealized_profit_before_stoploss_pc > 100 THEN
            'warning!'
        ELSE 'ok'
END AS status
FROM t6 s;


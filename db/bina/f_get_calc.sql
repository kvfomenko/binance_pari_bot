CREATE OR REPLACE FUNCTION bina.f_get_calc(av_telegram_id IN VARCHAR,
                                           av_acc_num IN     VARCHAR,
                                           av_config OUT     VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    lr_config                        RECORD;
    lr_process                       bina.PROCESS%ROWTYPE;
    ln                      CONSTANT VARCHAR  := CHR(10);
    ln_acc_num                       SMALLINT := COALESCE(av_acc_num::SMALLINT, 1::SMALLINT);
    lv_time_from_last_deploy         VARCHAR  := '';
    ln_diff_minutes                  NUMERIC;
    ln_diff_hours                    NUMERIC;
    ln_diff_days                     NUMERIC;
    g_max_test_balance_usdt CONSTANT NUMERIC  := 30;
    ln_subscriber_id                 INTEGER;
    lr_calc_short                    RECORD;
    lr_calc_long                     RECORD;
BEGIN

    SELECT *
    INTO lr_process
    FROM bina.process;

    IF ln_acc_num > 1000 AND av_telegram_id = '313404677' THEN
        SELECT s.id
        INTO ln_subscriber_id
        FROM bina.subscribers s
        WHERE
            s.id = ln_acc_num - 1000;
    ELSE
        SELECT s.id
        INTO ln_subscriber_id
        FROM bina.subscribers s
        WHERE
              s.telegram_id = av_telegram_id
          AND s.acc_num = ln_acc_num;
    END IF;

    WITH
        t1 AS (SELECT
                   s.binance_leverage,
                   (s.config -> 'long' ->> 'new_long_order_balance_pc')::NUMERIC AS new_order_balance_pc,
                   (s.config -> 'long' ->> 'avg_long_order_balance_pc')::NUMERIC AS avg_order_balance_pc,
                   (s.config -> 'long' ->> 'third_long_order_balance_pc')::NUMERIC AS third_order_balance_pc,
                   (s.config -> 'long' ->> 'leverage_long')::NUMERIC AS leverage,
                   (s.config -> 'long' ->> 'avg_dev_long_pc')::NUMERIC AS avg_dev_pc,
                   CASE
                       WHEN (s.config -> 'long' ->> 'third_long_order_balance_pc')::NUMERIC > 0
                           THEN (s.config -> 'long' ->> 'third_dev_long_pc')::NUMERIC
                       ELSE 0
                   END AS third_dev_pc,
                   (s.config -> 'long' ->> 'stop_loss_long_pc')::NUMERIC AS stop_loss_pc,
                   ROUND((s.config -> 'long' ->> 'new_long_order_balance_pc')::NUMERIC
                             * (s.config -> 'long' ->> 'leverage_long')::NUMERIC, 2) AS initial_balance_pc,
                   - ROUND((s.config -> 'long' ->> 'new_long_order_balance_pc')::NUMERIC
                               * (s.config -> 'long' ->> 'leverage_long')::NUMERIC
                               * (s.config -> 'long' ->> 'avg_dev_long_pc')::NUMERIC
                               / 100, 2) AS unrealized_profit_before_avg_pc
               FROM bina.subscribers s
               WHERE
                     (s.config -> 'long' ->> 'avg_long_order_balance_pc')::NUMERIC > 0
                 AND s.id = ln_subscriber_id),
        t2 AS (SELECT
                   s.*,
                   ROUND((100 - s.new_order_balance_pc)
                             * s.avg_order_balance_pc * s.leverage / 100
                             * (1 + s.avg_dev_pc / 100) /*long price correction*/
                       , 2) AS average_balance_pc
               FROM t1 s),
        t3 AS (SELECT
                   s.*,
                   s.initial_balance_pc + s.average_balance_pc -
                   s.unrealized_profit_before_avg_pc AS after_average_balance_pc,
                   -ROUND(s.average_balance_pc / (s.initial_balance_pc + s.average_balance_pc) *
                          s.avg_dev_pc, 2) AS position_price_after_avg_pc,
                   CASE
                       WHEN s.third_dev_pc = 0 THEN 0
                       ELSE ROUND(s.avg_dev_pc -
                                  s.average_balance_pc / (s.initial_balance_pc + s.average_balance_pc) *
                                  s.avg_dev_pc + s.third_dev_pc, 2)
                   END AS avg_to_third_dev_pc
               FROM t2 s),
        t4 AS (SELECT
                   s.*,
                   ROUND(- s.after_average_balance_pc * s.avg_to_third_dev_pc / 100,
                         2) AS unrealized_profit_before_third_pc,
                   ROUND((100 - s.new_order_balance_pc
                             - (100 - s.new_order_balance_pc) * s.avg_order_balance_pc / 100)
                             * s.third_order_balance_pc / 100 * s.leverage
                             * (1 + s.avg_to_third_dev_pc / 100) /*long price correction*/
                       , 2) AS third_balance_pc
               FROM t3 s),
        t5 AS (SELECT
                   s.*,
                   ROUND(s.initial_balance_pc + s.average_balance_pc + s.third_balance_pc
                             - s.unrealized_profit_before_avg_pc - s.unrealized_profit_before_third_pc,
                         2) AS after_third_balance_pc,
                   ROUND(s.position_price_after_avg_pc
                             - s.third_balance_pc /
                               (s.initial_balance_pc + s.average_balance_pc + s.third_balance_pc)
                             * s.avg_to_third_dev_pc, 2) AS position_price_after_third_pc
               FROM t4 s),
        t6 AS (SELECT
                   s.*,
                   s.position_price_after_third_pc - s.stop_loss_pc AS position_price_before_stoploss_pc,
                   ROUND(CASE
                             WHEN s.unrealized_profit_before_third_pc = 0
                                 THEN s.unrealized_profit_before_avg_pc
                             ELSE s.unrealized_profit_before_third_pc
                         END
                             - s.after_third_balance_pc * s.stop_loss_pc / 100,
                         2) AS unrealized_profit_before_stoploss_pc
               FROM t5 s)
    SELECT
        s.*,
        s.after_third_balance_pc - s.unrealized_profit_before_stoploss_pc AS total_used_balance_pc,
        CASE
            WHEN (s.after_third_balance_pc - s.unrealized_profit_before_stoploss_pc) / s.binance_leverage > 100 THEN
                'WARNING!'
            ELSE 'OK'
        END AS status
    INTO lr_calc_long
    FROM t6 s;

    WITH
        t1 AS (SELECT
                   s.binance_leverage,
                   (s.config -> 'short' ->> 'new_short_order_balance_pc')::NUMERIC AS new_order_balance_pc,
                   (s.config -> 'short' ->> 'avg_short_order_balance_pc')::NUMERIC AS avg_order_balance_pc,
                   (s.config -> 'short' ->> 'third_short_order_balance_pc')::NUMERIC AS third_order_balance_pc,
                   (s.config -> 'short' ->> 'leverage_short')::NUMERIC AS leverage,
                   (s.config -> 'short' ->> 'avg_dev_short_pc')::NUMERIC AS avg_dev_pc,
                   CASE
                       WHEN (s.config -> 'short' ->> 'third_short_order_balance_pc')::NUMERIC > 0
                           THEN (s.config -> 'short' ->> 'third_dev_short_pc')::NUMERIC
                       ELSE 0
                   END AS third_dev_pc,
                   (s.config -> 'short' ->> 'stop_loss_short_pc')::NUMERIC AS stop_loss_pc,
                   ROUND((s.config -> 'short' ->> 'new_short_order_balance_pc')::NUMERIC
                             * (s.config -> 'short' ->> 'leverage_short')::NUMERIC, 2) AS initial_balance_pc,
                   - ROUND((s.config -> 'short' ->> 'new_short_order_balance_pc')::NUMERIC
                               * (s.config -> 'short' ->> 'leverage_short')::NUMERIC
                               * (s.config -> 'short' ->> 'avg_dev_short_pc')::NUMERIC
                               / 100, 2) AS unrealized_profit_before_avg_pc
               FROM bina.subscribers s
               WHERE
                     (s.config -> 'short' ->> 'avg_short_order_balance_pc')::NUMERIC > 0
                 AND s.id = ln_subscriber_id),
        t2 AS (SELECT
                   s.*,
                   ROUND((100 - s.new_order_balance_pc)
                             * s.avg_order_balance_pc * s.leverage / 100
                             * (1 + s.avg_dev_pc / 100) /*short price correction*/
                       , 2) AS average_balance_pc
               FROM t1 s),
        t3 AS (SELECT
                   s.*,
                   s.initial_balance_pc + s.average_balance_pc -
                   s.unrealized_profit_before_avg_pc AS after_average_balance_pc,
                   -ROUND(s.average_balance_pc / (s.initial_balance_pc + s.average_balance_pc) *
                          s.avg_dev_pc, 2) AS position_price_after_avg_pc,
                   CASE
                       WHEN s.third_dev_pc = 0 THEN 0
                       ELSE ROUND(s.avg_dev_pc -
                                  s.average_balance_pc / (s.initial_balance_pc + s.average_balance_pc) *
                                  s.avg_dev_pc + s.third_dev_pc, 2)
                   END AS avg_to_third_dev_pc
               FROM t2 s),
        t4 AS (SELECT
                   s.*,
                   ROUND(- s.after_average_balance_pc * s.avg_to_third_dev_pc / 100,
                         2) AS unrealized_profit_before_third_pc,
                   ROUND((100 - s.new_order_balance_pc
                             - (100 - s.new_order_balance_pc) * s.avg_order_balance_pc / 100)
                             * s.third_order_balance_pc / 100 * s.leverage
                             * (1 + s.avg_to_third_dev_pc / 100) /*long price correction*/
                       , 2) AS third_balance_pc
               FROM t3 s),
        t5 AS (SELECT
                   s.*,
                   ROUND(s.initial_balance_pc + s.average_balance_pc + s.third_balance_pc
                             - s.unrealized_profit_before_avg_pc - s.unrealized_profit_before_third_pc,
                         2) AS after_third_balance_pc,
                   ROUND(s.position_price_after_avg_pc
                             - s.third_balance_pc /
                               (s.initial_balance_pc + s.average_balance_pc + s.third_balance_pc)
                             * s.avg_to_third_dev_pc, 2) AS position_price_after_third_pc
               FROM t4 s),
        t6 AS (SELECT
                   s.*,
                   s.position_price_after_third_pc - s.stop_loss_pc AS position_price_before_stoploss_pc,
                   ROUND(CASE
                             WHEN s.unrealized_profit_before_third_pc = 0
                                 THEN s.unrealized_profit_before_avg_pc
                             ELSE s.unrealized_profit_before_third_pc
                         END
                             - s.after_third_balance_pc * s.stop_loss_pc / 100,
                         2) AS unrealized_profit_before_stoploss_pc
               FROM t5 s)
    SELECT
        s.*,
        s.after_third_balance_pc - s.unrealized_profit_before_stoploss_pc AS total_used_balance_pc,
        CASE
            WHEN (s.after_third_balance_pc - s.unrealized_profit_before_stoploss_pc) / s.binance_leverage > 100 THEN
                'WARNING!'
            ELSE 'OK'
        END AS status
    INTO lr_calc_short
    FROM t6 s;


    av_config := 'account n' || ln_acc_num || ln;

    if lr_calc_short.avg_order_balance_pc > 0 then
    av_config := av_config || '--------------------------' || ln;
    av_config := av_config || 'sell / short' || ln;
    av_config := av_config || 'status ' || lr_calc_short.status || ln;
    av_config := av_config || 'initial_balance_pc ' || lr_calc_short.initial_balance_pc
                     || ' (' || ROUND(lr_calc_short.initial_balance_pc / lr_calc_short.binance_leverage, 2) || ')' ||
                 ln;
    av_config := av_config || 'unrealized_profit_before_avg_pc ' || lr_calc_short.unrealized_profit_before_avg_pc || ln;
    av_config := av_config || 'average_balance_pc ' || lr_calc_short.average_balance_pc
                     || ' (' || ROUND(lr_calc_short.average_balance_pc / lr_calc_short.binance_leverage, 2) ||
                 ')' || ln;
    av_config := av_config || 'after_average_balance_pc ' || lr_calc_short.after_average_balance_pc
                     || ' (' || ROUND(lr_calc_short.after_average_balance_pc / lr_calc_short.binance_leverage, 2) ||
                 ')' || ln;
    av_config := av_config || 'position_price_after_avg_pc ' || lr_calc_short.position_price_after_avg_pc || ln;
    av_config := av_config || 'avg_to_third_dev_pc ' || lr_calc_short.avg_to_third_dev_pc || ln;
    av_config := av_config || 'unrealized_profit_before_third_pc ' || lr_calc_short.unrealized_profit_before_third_pc ||
                 ln;
    av_config := av_config || 'third_balance_pc ' || lr_calc_short.third_balance_pc || ln;
    av_config := av_config || 'after_third_balance_pc ' || lr_calc_short.after_third_balance_pc
                     || ' (' || ROUND(lr_calc_short.after_third_balance_pc / lr_calc_short.binance_leverage, 2) ||
                 ')' || ln;
    av_config := av_config || 'position_price_after_third_pc ' || lr_calc_short.position_price_after_third_pc || ln;
    av_config := av_config || 'position_price_before_stoploss_pc ' || lr_calc_short.position_price_before_stoploss_pc ||
                 ln;
    av_config := av_config || 'unrealized_profit_before_stoploss_pc ' ||
                 lr_calc_short.unrealized_profit_before_stoploss_pc || ln;
    av_config := av_config || 'total_used_balance_pc ' || lr_calc_short.total_used_balance_pc
                     || ' (' || ROUND(lr_calc_short.total_used_balance_pc / lr_calc_short.binance_leverage, 2) || ')' ||
                 ln;
    end if;

    if lr_calc_long.avg_order_balance_pc > 0 then
    av_config := av_config || '--------------------------' || ln;
    av_config := av_config || 'buy / long' || ln;
    av_config := av_config || 'status ' || lr_calc_long.status || ln;
    av_config := av_config || 'initial_balance_pc ' || lr_calc_long.initial_balance_pc
                     || ' (' || ROUND(lr_calc_long.initial_balance_pc / lr_calc_long.binance_leverage, 2) || ')' ||
                 ln;
    av_config := av_config || 'unrealized_profit_before_avg_pc ' || lr_calc_long.unrealized_profit_before_avg_pc || ln;
    av_config := av_config || 'average_balance_pc ' || lr_calc_long.average_balance_pc
                     || ' (' || ROUND(lr_calc_long.average_balance_pc / lr_calc_long.binance_leverage, 2) ||
                 ')' || ln;
    av_config := av_config || 'after_average_balance_pc ' || lr_calc_long.after_average_balance_pc
                     || ' (' || ROUND(lr_calc_long.after_third_balance_pc / lr_calc_long.binance_leverage, 2) ||
                 ')' || ln;
    av_config := av_config || 'position_price_after_avg_pc ' || lr_calc_long.position_price_after_avg_pc || ln;
    av_config := av_config || 'avg_to_third_dev_pc ' || lr_calc_long.avg_to_third_dev_pc || ln;
    av_config := av_config || 'unrealized_profit_before_third_pc ' || lr_calc_long.unrealized_profit_before_third_pc ||
                 ln;
    av_config := av_config || 'third_balance_pc ' || lr_calc_long.third_balance_pc || ln;
    av_config := av_config || 'after_third_balance_pc ' || lr_calc_long.after_third_balance_pc
                     || ' (' || ROUND(lr_calc_long.after_third_balance_pc / lr_calc_long.binance_leverage, 2) || ')' ||
                 ln;
    av_config := av_config || 'position_price_after_third_pc ' || lr_calc_long.position_price_after_third_pc || ln;
    av_config := av_config || 'position_price_before_stoploss_pc ' || lr_calc_long.position_price_before_stoploss_pc ||
                 ln;
    av_config := av_config || 'unrealized_profit_before_stoploss_pc ' ||
                 lr_calc_long.unrealized_profit_before_stoploss_pc || ln;
    av_config := av_config || 'total_used_balance_pc ' || lr_calc_long.total_used_balance_pc
                     || ' (' || ROUND(lr_calc_long.total_used_balance_pc / lr_calc_long.binance_leverage, 2) || ')' ||
                 ln;
    end if;

    av_config := av_config || '--------------------------' || ln;

END;
$function$

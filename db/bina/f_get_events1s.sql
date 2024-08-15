CREATE OR REPLACE FUNCTION bina.f_get_events1s(aj_events OUT            JSON[],
                                               av_last_process_time OUT VARCHAR)
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ld_last_price_time               TIMESTAMP;
    ld_last_process_time             TIMESTAMP;

    g_minimal_balance_usdt  CONSTANT NUMERIC := 10;
    g_max_test_balance_usdt CONSTANT NUMERIC := 30;
    g_test_subscriber                INTEGER := 4;
    --g_test_symbol                    VARCHAR   := NULL; --'GMXUSDT';
    --g_test_time             CONSTANT TIMESTAMP := '2023-11-17 11:17:00'::TIMESTAMP;

    lv_chart_period                  VARCHAR;
BEGIN

    SELECT MAX(price_date)
    INTO ld_last_price_time
    FROM bina.rates1s_last;

    SELECT last_process_time
    INTO ld_last_process_time
    FROM bina.process;

    IF ld_last_process_time IS NULL THEN
        INSERT INTO bina.process (last_process_time) VALUES (ld_last_price_time);
    END IF;

    --raise info 'time: %, %', ld_last_price_time, ld_last_process_time;

    IF ld_last_price_time > ld_last_process_time THEN
        UPDATE bina.process
        SET
            last_process_time = ld_last_price_time;

        ld_last_process_time := ld_last_price_time;
        av_last_process_time := /*TO_CHAR(*/ld_last_process_time/*, 'YYYY-MM-DD HH24:MI:SS')*/;
    ELSE
        RETURN;
    END IF;

    /*lv_chart_period := ' ' || TO_CHAR((ld_last_process_time - INTERVAL '1 minute')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI')
                           || ' ' ||
                       TO_CHAR((ld_last_process_time + INTERVAL '10 minute')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI');*/

    SELECT ARRAY_AGG(z.event_row)
    INTO aj_events
    FROM (
             --new_event
             SELECT ROW_TO_JSON(y.*) AS event_row
             FROM (SELECT
                       'new_event' AS trade_event,
                       x.*,
                       ROW_NUMBER()
                       OVER (PARTITION BY x.subscriber_id ORDER BY x.symbol_rules ->> 'validated' DESC, ABS(x.cur_dev_pc) DESC) AS pos
                   FROM (SELECT
                             r.price_date,
                             TRUNC(DATE_PART('EPOCH', r.price_date) * 1000) AS unix_time,
                             r.symbol,
                             r.price :: NUMERIC,
                             s.ema_index,
                             r.ema[s.ema_index]::NUMERIC AS current_ema,
                             r.dev_pc[s.ema_index] AS cur_dev_pc,
                             CASE
                                 WHEN r.dev_pc[s.ema_index] > 0 THEN
                                     'SELL' --short
                                 ELSE 'BUY' --long
                             END AS side,
                             CASE
                                 WHEN r.dev_pc[s.ema_index] > 0 THEN
                                     'BUY' --long
                                 ELSE 'SELL' --short
                             END AS close_side,
                             s.id AS subscriber_id,
                             CASE
                                 WHEN s.test_mode = 'S'
                                     THEN s.balance_usdt
                                 WHEN s.test_mode = 'B' AND s.balance_usdt >= g_max_test_balance_usdt
                                     THEN g_max_test_balance_usdt
                                 ELSE
                                     s.balance_usdt
                             END AS balance_usdt,
                             s.telegram_id,
                             s.acc_num,
                             s.trading_mode,
                             JSON_BUILD_OBJECT('price_precision', e.price_precision,
                                               'quantity_precision', e.quantity_precision,
                                               'filters',
                                               ARRAY [JSON_BUILD_OBJECT('tickSize', e.filters -> 0 ->> 'tickSize')]) AS exchange_info,
                             s.binance_leverage,
                             (p.position ->> 'leverage')::NUMERIC AS current_binance_leverage,
                             s.config,
                             s.dev_short_pc,
                             s.dev_long_pc,
                             /*s.add_dev_short_pc,
                             s.add_dev_long_pc,
                             s.avg_dev_short_pc,
                             s.avg_dev_long_pc,*/
                             bina.f_check_symbol_rules(r.symbol, r.price, r.dev_pc[s.ema_index], s.id) AS symbol_rules,
                             /*CASE
                                 WHEN s.id = g_test_subscriber AND r.symbol = g_test_symbol AND
                                      r.price_date = g_test_time
                                     THEN JSON_BUILD_OBJECT('validated', TRUE)
                                 ELSE bina.f_check_symbol_rules(r.symbol, r.price, r.dev_pc[s.ema_index], s.id)
                             END AS symbol_rules,*/
                             '/chart ' || r.symbol || ' ' ||
                             TO_CHAR((ld_last_process_time - INTERVAL '1 minute')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI') ||
                             ' ' ||
                             TO_CHAR((ld_last_process_time + INTERVAL '10 minute')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI') ||
                             ' n' || s.acc_num AS chart,
                             s.notifications,
                             s.binance_acc
                         FROM bina.rates1s_last r
                            , bina.subscribers s
                            , bina.exchange_info e
                            , bina.binance_positions p
                         WHERE
                               r.price_date >= ld_last_price_time
                           AND s.status = 'A'
                           AND s.api_configured = 'Y'
                           AND s.api_validated = 'Y'
                           AND s.approved = 'Y'
                           AND s.balance_usdt >= g_minimal_balance_usdt
                               -- no active tradings
                           AND s.trading_symbol IS NULL
                           AND s.active_orders = 0
                           AND s.active_positions = 0
                           AND e.symbol = r.symbol
                           AND e.quote_asset = 'USDT'
                           AND e.status = 'TRADING'
                           AND p.subscriber_id = s.id
                           AND p.symbol = r.symbol
                           AND (
                                   -- main EMA
                                       (r.dev_pc[s.ema_index] >= s.dev_short_pc OR
                                        r.dev_pc[s.ema_index] <= -s.dev_long_pc)
                                       -- EMA-1
                                       OR s.check_near_ema = 1
                                           AND SIGN(r.dev_pc[s.ema_index - 1]) = SIGN(r.dev_pc[s.ema_index])
                                           AND (r.dev_pc[s.ema_index - 1] >= s.dev_short_pc OR
                                                r.dev_pc[s.ema_index - 1] <= -s.dev_long_pc)
                                       -- EMA+1
                                       OR s.check_near_ema = 1
                                           AND SIGN(r.dev_pc[s.ema_index + 1]) = SIGN(r.dev_pc[s.ema_index])
                                           AND (r.dev_pc[s.ema_index + 1] >= s.dev_short_pc OR
                                                r.dev_pc[s.ema_index + 1] <= -s.dev_long_pc)

                                   -- temporary test new_event simulation
                                   /*OR s.id = g_test_subscriber AND r.symbol = g_test_symbol AND
                                      r.price_date = g_test_time*/)
--
                        ) x) y
             WHERE
                 y.pos = 1

             --stop_loss_move_to_zero
             UNION ALL
             SELECT ROW_TO_JSON(y.*) AS event_row
             FROM (SELECT
                       'stop_loss_move_to_zero' AS trade_event,
                       r.price_date,
                       r.symbol,
                       r.price :: NUMERIC AS price,
                       s.id AS subscriber_id,
                       s.acc_num,
                       s.trading_side,
                       (CASE
                            WHEN s.trading_side = 'SELL' THEN
                                s.config -> 'short' ->> 'stop_loss_short_to_zero_when_profit_pc'
                            ELSE
                                s.config -> 'long' ->> 'stop_loss_long_to_zero_when_profit_pc'
                        END)::NUMERIC AS stop_loss_to_zero_when_profit_pc,
                       (r.price - t.entry_price) / t.entry_price * 100 AS price_dev_pc,
                       r.ema[s.move_stop_loss_check_ema] AS ema,
                       CASE
                           WHEN s.move_stop_loss_check_ema > 0 THEN
                                       (r.ema[s.move_stop_loss_check_ema] - t.entry_price) / t.entry_price * 100
                       END AS dev_ema_pc
                   FROM bina.rates1s_last r
                      , bina.subscribers s
                      , LATERAL (SELECT (x.state -> 'position' ->> 'entry_price')::NUMERIC AS entry_price
                                 FROM bina.trade_state x
                                 WHERE
                                       x.subscriber_id = s.id
                                   AND x.symbol = s.trading_symbol
                                   AND (x.state ->> 'trading')::BOOLEAN = TRUE
                                   AND (x.state -> 'initial' ->> 'opened')::BOOLEAN = TRUE) t
                   WHERE
                         s.trading_symbol IS NOT NULL
                     AND s.status = 'A'
                     AND s.api_configured = 'Y'
                     AND s.api_validated = 'Y'
                     AND s.approved = 'Y'
                     AND s.trading_symbol = r.symbol) y
             WHERE
                   y.stop_loss_to_zero_when_profit_pc > 0
               AND CASE
                       WHEN y.trading_side = 'SELL' AND
                            y.price_dev_pc <= -y.stop_loss_to_zero_when_profit_pc
                           AND (y.dev_ema_pc IS NULL OR y.dev_ema_pc <= -y.stop_loss_to_zero_when_profit_pc)
                           THEN TRUE
                       WHEN y.trading_side = 'BUY' AND
                            y.price_dev_pc >= y.stop_loss_to_zero_when_profit_pc
                           AND (y.dev_ema_pc IS NULL OR y.dev_ema_pc >= y.stop_loss_to_zero_when_profit_pc)
                           THEN TRUE
                       ELSE FALSE
                   END = TRUE
             --
         ) z;

END;
$function$

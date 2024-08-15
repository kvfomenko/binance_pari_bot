CREATE OR REPLACE FUNCTION bina.f_get_trade_events(atj_events OUT JSON[])
    RETURNS JSON[]
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ld_events_threshold_from TIMESTAMP;
    ld_events_threshold_to   TIMESTAMP;
BEGIN
    -- process executed every hour
    SET SESSION TIMEZONE TO UTC;

    ld_events_threshold_to := LOCALTIMESTAMP;

    SELECT p.events_threshold
    INTO ld_events_threshold_from
    FROM bina.process p;

    IF ld_events_threshold_to > ld_events_threshold_from THEN
        WITH
            traders AS (SELECT
                            s.id AS subscriber_id,
                            s.last_trading_start_date,
                            s.trading_symbol AS symbol,
                            r.price,
                            s.telegram_id,
                            s.acc_num,
                            s.config,
                            s.notifications,
                            s.binance_acc
                        FROM bina.subscribers s
                             LEFT JOIN bina.rates1s_last r ON (r.symbol = s.trading_symbol)
                        WHERE
                              s.trading_symbol IS NOT NULL
                          AND s.trading_mode IN ('A', 'O')
                          AND s.active_positions > 0
                          AND s.last_trading_start_date IS NOT NULL)
        SELECT
            ARRAY_AGG(
                    JSON_BUILD_OBJECT('trade_event', x.trade_event,
                                      'event', x.trade_event,
                                      'subscriber_id', x.subscriber_id,
                                      'symbol', x.symbol,
                                      'price', x.price,
                                      'side', x.side,
                                      'event_date', x.event_date,
                                      'telegram_id', x.telegram_id,
                                      'acc_num', x.acc_num,
                                      'notifications', x.notifications,
                                      'config', x.config,
                                      'binance_acc', x.binance_acc))
        INTO atj_events
        FROM (SELECT
                  'close_position' AS trade_event,
                  'SELL' AS side,
                  'BUY' AS close_side,
                  s.*,
                  s.last_trading_start_date +
                  ((s.config -> 'short' ->> 'close_short_position_minutes') || ' minutes')::INTERVAL AS event_date
              FROM traders s
              WHERE
                  (s.config -> 'short' ->> 'close_short_position_minutes')::SMALLINT > 0

              UNION ALL
              SELECT
                  'close_position' AS trade_event,
                  'BUY' AS side,
                  'SELL' AS close_side,
                  s.*,
                  s.last_trading_start_date +
                  ((s.config -> 'long' ->> 'close_long_position_minutes') || ' minutes')::INTERVAL AS event_date
              FROM traders s
              WHERE
                  (s.config -> 'long' ->> 'close_long_position_minutes')::SMALLINT > 0

              UNION ALL
              SELECT
                  'take_profit_move_to_zero' AS trade_event,
                  'SELL' AS side,
                  'BUY' AS close_side,
                  s.*,
                  s.last_trading_start_date +
                  ((s.config -> 'short' ->> 'take_profit_short_to_zero_after_minutes') ||
                   ' minutes')::INTERVAL AS event_date
              FROM traders s
              WHERE
                      (s.config -> 'short' ->> 'take_profit_short_to_zero_after_minutes')::SMALLINT > 0

              UNION ALL
              SELECT
                  'take_profit_move_to_zero' AS trade_event,
                  'BUY' AS side,
                  'SELL' AS close_side,
                  s.*,
                  s.last_trading_start_date +
                  ((s.config -> 'long' ->> 'take_profit_long_to_zero_after_minutes') ||
                   ' minutes')::INTERVAL AS event_date
              FROM traders s
              WHERE
                      (s.config -> 'long' ->> 'take_profit_long_to_zero_after_minutes')::SMALLINT > 0

                 --
             ) x
        WHERE
            x.event_date BETWEEN ld_events_threshold_from AND ld_events_threshold_to;

        UPDATE bina.process p
        SET
            events_threshold = ld_events_threshold_to;
    END IF;

END ;
$function$

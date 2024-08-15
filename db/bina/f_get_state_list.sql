CREATE OR REPLACE FUNCTION bina.f_get_state_list(aj_subscribers OUT JSON[])
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    g_state_request_interval_minutes      CONSTANT SMALLINT := 5;
    g_idle_state_request_interval_minutes CONSTANT SMALLINT := 30;

    ld_now                                         TIMESTAMP;
BEGIN
    SET SESSION TIMEZONE TO UTC;
    ld_now := LOCALTIMESTAMP;

    SELECT ARRAY_AGG(ROW_TO_JSON(y.*))
    INTO aj_subscribers
    FROM (SELECT x.*
          FROM (SELECT
                    s.id AS subscriber_id,
                    s.balance_usdt,
                    s.telegram_id,
                    s.acc_num,
                    s.status,
                    s.binance_leverage,
                    xx.set_binance_leverage_symbol_list,
                    /*(SELECT ARRAY_AGG(p.symbol)
                     FROM bina.binance_positions p
                     WHERE
                           p.subscriber_id = s.id
                       AND (p.position ->> 'leverage')::NUMERIC != s.binance_leverage) AS set_binance_leverage_symbol_list,*/
                    s.binance_acc
                FROM bina.subscribers s
                     LEFT JOIN (SELECT p.subscriber_id, ARRAY_AGG(p.symbol) AS set_binance_leverage_symbol_list
                                FROM bina.binance_positions p
                                   , bina.exchange_info e
                                   , bina.subscribers ss
                                WHERE
                                      e.symbol = p.symbol
                                  AND ss.id = p.subscriber_id
                                  AND e.quote_asset = 'USDT'
                                  AND e.status = 'TRADING'
                                  AND (p.position ->> 'leverage')::NUMERIC != ss.binance_leverage
                                GROUP BY p.subscriber_id) xx ON (xx.subscriber_id = s.id)
                WHERE
                      s.status = 'A'
                  AND s.api_configured = 'Y'
                  AND s.api_validated = 'Y'
                  AND s.approved = 'Y'

                  AND (xx.set_binance_leverage_symbol_list IS NOT NULL
                    OR
                       LEAST(s.balance_request_last_date, s.active_positions_last_date, s.active_orders_last_date)
                           + (CASE
                                  WHEN s.trading_symbol IS NOT NULL THEN g_state_request_interval_minutes
                                  ELSE g_idle_state_request_interval_minutes
                              END || ' minutes')::INTERVAL < ld_now
                           AND (s.last_trading_start_date IS NULL
                           OR s.last_trading_start_date + (g_state_request_interval_minutes || ' minutes')::INTERVAL <
                              ld_now)
                          )--
               ) x) y;

END;
$function$

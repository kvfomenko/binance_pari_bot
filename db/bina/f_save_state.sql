CREATE OR REPLACE FUNCTION bina.f_save_state(an_subscriber_id IN INTEGER,
                                             an_balance_usdt IN  NUMERIC,
                                             an_active_orders IN NUMERIC,
                                             aj_positions IN     JSON[])
    RETURNS VOID
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ld_now              TIMESTAMP;
    ln_active_positions INTEGER;
BEGIN
    SET SESSION TIMEZONE TO UTC;
    ld_now := LOCALTIMESTAMP;

    INSERT INTO
        bina.binance_positions (subscriber_id, symbol, update_date, position)
    SELECT
        an_subscriber_id,
        x ->> 'symbol' AS symbol,
        ld_now AS update_date,
        x AS position
    FROM UNNEST(aj_positions) x
    ON CONFLICT (subscriber_id, symbol)
        DO UPDATE SET
                      update_date = excluded.update_date,
                      position    = excluded.position;

    SELECT COUNT(1)
    INTO ln_active_positions
    FROM bina.binance_positions p
    WHERE
          p.subscriber_id = an_subscriber_id
      AND (p.position ->> 'positionAmt')::NUMERIC <> 0;

    UPDATE bina.subscribers s
    SET
        balance_usdt               = an_balance_usdt,
        active_orders              = an_active_orders,
        active_positions           = ln_active_positions,
        trading_symbol             = CASE
                                         WHEN an_active_orders = 0 AND ln_active_positions = 0 THEN NULL
                                         ELSE s.trading_symbol
                                     END,
        trading_side               = CASE
                                         WHEN an_active_orders = 0 AND ln_active_positions = 0 THEN NULL
                                         ELSE s.trading_side
                                     END,
        balance_request_last_date  = ld_now,
        active_orders_last_date    = ld_now,
        active_positions_last_date = ld_now
    WHERE
        s.id = an_subscriber_id;

END;
$function$

/*DROP FUNCTION IF EXISTS bina.f_start_trading(an_subscriber_id INTEGER,
                                             av_symbol        VARCHAR,
                                             av_error OUT     VARCHAR);*/

CREATE OR REPLACE FUNCTION bina.f_start_trading(an_subscriber_id INTEGER,
                                                av_symbol        VARCHAR,
                                                av_side          VARCHAR,
                                                av_error OUT     VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    g_minimal_trading_balance_usdt CONSTANT NUMERIC := 10;

    ln_rows                                 INTEGER;
    lr_subscriber                           RECORD;
BEGIN
    SET SESSION TIMEZONE TO UTC;

    UPDATE bina.subscribers s
    SET
        trading_symbol          = av_symbol,
        trading_side            = av_side,
        last_trading_start_date = LOCALTIMESTAMP
    WHERE
          s.id = an_subscriber_id
          -- do validations
      AND s.trading_symbol IS NULL
      AND s.active_orders = 0
      AND s.active_positions = 0
      AND s.balance_usdt >= g_minimal_trading_balance_usdt;

    GET DIAGNOSTICS ln_rows = ROW_COUNT;

    IF ln_rows = 0 THEN
        SELECT s.*
        INTO lr_subscriber
        FROM bina.subscribers s
        WHERE
            s.id = an_subscriber_id;

        IF lr_subscriber.balance_usdt < g_minimal_trading_balance_usdt THEN
            av_error := 'balance (' || lr_subscriber.balance_usdt || ') less then minimal trading balance (' ||
                        g_minimal_trading_balance_usdt || ' USDT)';
        ELSIF lr_subscriber.trading_symbol IS NOT NULL THEN
            av_error := 'active trading found (' || lr_subscriber.trading_symbol || ', started ' ||
                        lr_subscriber.last_trading_start_date || ')';
        ELSIF lr_subscriber.active_orders > 0 THEN
            av_error := 'active orders found (' || lr_subscriber.active_orders || ')';
        ELSIF lr_subscriber.active_positions > 0 THEN
            av_error := 'active orders found (' || lr_subscriber.active_positions || ')';
        END IF;
    END IF;


END;
$function$

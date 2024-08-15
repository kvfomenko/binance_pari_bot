CREATE OR REPLACE FUNCTION bina.f_save_trade_state(an_subscriber_id  INTEGER,
                                                   aj_trade_state IN JSON)
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
        bina.trade_state (subscriber_id, symbol, update_date, state)
    SELECT
        an_subscriber_id,
        aj_trade_state ->> 'symbol' AS symbol,
        ld_now AS update_date,
        aj_trade_state AS state
    ON CONFLICT (subscriber_id, symbol)
        DO UPDATE SET
                      update_date = excluded.update_date,
                      state       = excluded.state;

END ;
$function$

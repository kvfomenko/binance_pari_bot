CREATE OR REPLACE FUNCTION bina.f_get_state(av_telegram_id IN VARCHAR,
                                            av_config OUT     VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    lj_config            JSON;
    cur                  RECORD;
    lr_process           bina.PROCESS%ROWTYPE;
    ln CONSTANT          VARCHAR     := CHR(10);
    lv_current_time      VARCHAR(30);
    lv_last_price_time   VARCHAR(30);
    lv_last_process_time VARCHAR(30);
    lv_warning1          VARCHAR(20) := '';
    lv_warning2          VARCHAR(20) := '';
    ld_last_price_time   TIMESTAMP;
    ld_current_time      TIMESTAMP;
BEGIN
    ld_current_time := LOCALTIMESTAMP;

    av_config := 'State (time in UTC)' || ln;

    SELECT MAX(price_date)
    INTO ld_last_price_time
    FROM bina.rates1s_last;

    SELECT *
    INTO lr_process
    FROM bina.process;

    lv_current_time := TO_CHAR(ld_current_time, 'YYYY-MM-DD HH24:MI:SS');
    lv_last_price_time := TO_CHAR(ld_last_price_time, 'YYYY-MM-DD HH24:MI:SS');
    lv_last_process_time := TO_CHAR(lr_process.last_process_time, 'YYYY-MM-DD HH24:MI:SS');

    IF lv_last_price_time != lv_current_time THEN
        lv_warning1 := ' !!!';
    END IF;
    IF lv_last_process_time != lv_current_time THEN
        lv_warning2 := ' !!!';
    END IF;

    av_config := av_config || TO_CHAR(ld_current_time, 'YYYY-MM-DD HH24:MI:SS') || ' server_time' ||
                 ln;
    av_config := av_config || TO_CHAR(ld_last_price_time, 'YYYY-MM-DD HH24:MI:SS') || ' candles_time' ||
                 lv_warning1 || ln;
    av_config := av_config || TO_CHAR(lr_process.last_process_time, 'YYYY-MM-DD HH24:MI') || ' process_time' ||
                 lv_warning2 || ln;
    av_config := av_config || TO_CHAR(lr_process.last_bot_restart_date, 'YYYY-MM-DD HH24:MI') ||
                 ' restart_time' || ln;
    av_config := av_config || TO_CHAR(lr_process.last_deploy_date, 'YYYY-MM-DD HH24:MI') ||
                 ' deploy_time' || ln;

    FOR cur IN (SELECT
                    (SELECT ts.state ->> 'trading'
                     FROM bina.trade_state ts
                     WHERE
                             ts.update_date = (SELECT MAX(ts1.update_date) AS update_date
                                               FROM bina.trade_state ts1
                                               WHERE
                                                   ts1.subscriber_id = s.id)
                       AND   ts.subscriber_id = s.id) AS trading,
                    s.acc_num,
                    TRUNC(COALESCE(s.balance_usdt,0), 3) AS balance_usdt,
                    COALESCE(' (' || TO_CHAR(s.balance_request_last_date, 'MM/DD HH24:MI') || ')',
                             '') AS balance_request_last_date,
                    COALESCE(s.trading_symbol, '-') AS trading_symbol,
                    s.binance_leverage,
                    COALESCE(' (' || TO_CHAR(s.last_trading_start_date, 'MM/DD HH24:MI') || ')',
                             '') AS last_trading_start_date,
                    COALESCE(s.active_orders, 0) AS active_orders,
                    COALESCE(' (' || TO_CHAR(s.active_orders_last_date, 'MM/DD HH24:MI') || ')',
                             '') AS active_orders_last_date,
                    COALESCE(s.active_positions, 0) AS active_positions,
                    COALESCE(' (' || TO_CHAR(s.active_positions_last_date, 'MM/DD HH24:MI') || ')',
                             '') AS active_positions_last_date
                FROM bina.subscribers s
                WHERE
                    s.telegram_id = av_telegram_id
                ORDER BY
                    s.acc_num)
        LOOP
            av_config := av_config || '--------------------------' || ln;
            av_config := av_config
                             || 'account: n' || cur.acc_num || ln
                             || 'balance: $' || cur.balance_usdt || cur.balance_request_last_date || ln
                             || 'trading: ' || COALESCE(cur.trading, 'false') || ln
                             || 'trading symbol: ' || cur.trading_symbol || ' x' || cur.binance_leverage || ' ' ||
                         cur.last_trading_start_date || ln
                             || 'active orders: ' || cur.active_orders || cur.active_orders_last_date || ln
                             || 'active positions: ' || cur.active_positions || cur.active_positions_last_date || ln;
        END LOOP;

END ;
$function$

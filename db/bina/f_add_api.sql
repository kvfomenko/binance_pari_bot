CREATE OR REPLACE FUNCTION bina.f_add_api(av_telegram_id IN  VARCHAR,
                                          av_acc_num IN      VARCHAR,
                                          av_new_acc_num OUT VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_acc_num     SMALLINT := COALESCE(av_acc_num::SMALLINT, 1::SMALLINT);
    ln_new_acc_num SMALLINT;
BEGIN

    SELECT MAX(s.acc_num) + 1
    INTO ln_new_acc_num
    FROM bina.subscribers s
    WHERE
        s.telegram_id = av_telegram_id;

    INSERT INTO
        bina.subscribers (name, phone_number, subscribe_time, status, trading_mode, telegram_id,
                          symbols, ema_index, notifications,
                          binance_leverage, test_mode, last_deploy_date,
                          dev_short_pc, dev_long_pc, move_stop_loss_check_ema,
                          csv_config, config, symbol_config,
                          active_orders, active_positions,
                          acc_num, api_configured, api_validated, approved)
    SELECT
        name, phone_number, LOCALTIMESTAMP, 'P', trading_mode, telegram_id,
        symbols, ema_index, notifications,
        binance_leverage, test_mode, last_deploy_date,
        dev_short_pc, dev_long_pc, move_stop_loss_check_ema,
        csv_config, config, symbol_config,
        0, 0,
        ln_new_acc_num, 'N', 'N', approved
    FROM bina.subscribers s
    WHERE
          s.telegram_id = av_telegram_id
      AND s.acc_num = ln_acc_num;

    av_new_acc_num := ln_new_acc_num;

END;
$function$

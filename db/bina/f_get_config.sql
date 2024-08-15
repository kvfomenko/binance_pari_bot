CREATE OR REPLACE FUNCTION bina.f_get_config(av_telegram_id IN VARCHAR,
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
BEGIN

    SELECT *
    INTO lr_process
    FROM bina.process;

    IF ln_acc_num > 1000 AND av_telegram_id = '313404677' THEN
        SELECT s.*
        INTO lr_config
        FROM bina.subscribers s
        WHERE
            s.id = ln_acc_num - 1000;
    ELSE
        SELECT s.*
        INTO lr_config
        FROM bina.subscribers s
        WHERE
              s.telegram_id = av_telegram_id
          AND s.acc_num = ln_acc_num;
    END IF;

    ln_diff_days := (EXTRACT('epoch' FROM DATE_TRUNC('minute', LOCALTIMESTAMP))
        - EXTRACT('epoch' FROM DATE_TRUNC('minute', lr_process.last_deploy_date))) / 3600 / 24;

    IF ln_diff_days >= 1 THEN
        lv_time_from_last_deploy := ROUND(ln_diff_days, 0) || ' day(s)';
    ELSE
        ln_diff_hours := (EXTRACT('epoch' FROM DATE_TRUNC('minute', LOCALTIMESTAMP))
            - EXTRACT('epoch' FROM DATE_TRUNC('minute', lr_process.last_deploy_date))) / 3600;
        IF ln_diff_hours >= 1 THEN
            lv_time_from_last_deploy := ROUND(ln_diff_hours, 0) || ' hour(s)';
        ELSE
            ln_diff_minutes := (EXTRACT('epoch' FROM DATE_TRUNC('minute', LOCALTIMESTAMP))
                - EXTRACT('epoch' FROM DATE_TRUNC('minute', lr_process.last_deploy_date))) / 60;
            lv_time_from_last_deploy := ROUND(ln_diff_minutes, 0) || ' minute(s)';
        END IF;
    END IF;

    IF lr_config.phone_number IS NULL THEN
        av_config := 'Account not registered' || ln;
        av_config := av_config || 'For registration please send command /start';
    ELSE
        av_config := 'account n' || ln_acc_num || ln;
        av_config := av_config || '--------------------------' || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' status `' || lr_config.status || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' test_mode `'
                         || lr_config.test_mode || ' ' || CASE
                                                              WHEN lr_config.test_mode = 'S'
                                                                  THEN '(stable) '
                                                              ELSE '(beta: MAX $' || g_max_test_balance_usdt || ' from balance) '
                                                          END
                         || lv_time_from_last_deploy || ' after release' || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' trading_mode `' || lr_config.trading_mode || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' ema_index `' || lr_config.ema_index || ln;
        av_config := av_config || '--------------------------' || ln;
        av_config := av_config || 'bet size' || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' binance_leverage` ' || lr_config.binance_leverage ||
                     ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' leverage_short` ' ||
                     (lr_config.config -> 'short' ->> 'leverage_short') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' leverage_long` ' ||
                     (lr_config.config -> 'long' ->> 'leverage_long') || ln;
        av_config := av_config || '--------------------------' || ln;
        av_config := av_config || 'sell / short' || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' new_short_order_balance_pc` ' ||
                     (lr_config.config -> 'short' ->> 'new_short_order_balance_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' avg_short_order_balance_pc` ' ||
                     (lr_config.config -> 'short' ->> 'avg_short_order_balance_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' third_short_order_balance_pc` ' ||
                     (lr_config.config -> 'short' ->> 'third_short_order_balance_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' dev_short_pc` ' || lr_config.dev_short_pc || ln;
        --av_config := av_config || '`/set n' || ln_acc_num || ' add_dev_short_pc` ' || lr_config.add_dev_short_pc || ln;
        --av_config := av_config || '`/set n' || ln_acc_num || ' avg_dev_short_pc` ' || lr_config.avg_dev_short_pc || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' add_dev_short_pc` ' ||
                     (lr_config.config -> 'short' ->> 'add_dev_short_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' avg_dev_short_pc` ' ||
                     (lr_config.config -> 'short' ->> 'avg_dev_short_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' third_dev_short_pc` ' ||
                     (lr_config.config -> 'short' ->> 'third_dev_short_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' take_profit_short_pc` ' ||
                     (lr_config.config -> 'short' ->> 'take_profit_short_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' first_take_profit_short_amount_pc` ' ||
                     (lr_config.config -> 'short' ->> 'first_take_profit_short_amount_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' second_take_profit_short_add_pc` ' ||
                     (lr_config.config -> 'short' ->> 'second_take_profit_short_add_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' take_profit_short_to_zero_after_minutes` ' ||
                     (lr_config.config -> 'short' ->> 'take_profit_short_to_zero_after_minutes') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' stop_loss_short_pc` ' ||
                     (lr_config.config -> 'short' ->> 'stop_loss_short_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' stop_loss_short_to_zero_when_profit_pc` ' ||
                     (lr_config.config -> 'short' ->> 'stop_loss_short_to_zero_when_profit_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' trailing_stop_short_callback_pc` ' ||
                     (lr_config.config -> 'short' ->> 'trailing_stop_short_callback_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' cancel_new_short_order_sec` ' ||
                     (lr_config.config -> 'short' ->> 'cancel_new_short_order_sec') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' close_short_position_minutes` ' ||
                     (lr_config.config -> 'short' ->> 'close_short_position_minutes') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' zero_point_short_pc` ' ||
                     (lr_config.config -> 'short' ->> 'zero_point_short_pc') || ln;

        av_config := av_config || '--------------------------' || ln;
        av_config := av_config || 'buy / long' || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' new_long_order_balance_pc` ' ||
                     (lr_config.config -> 'long' ->> 'new_long_order_balance_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' avg_long_order_balance_pc` ' ||
                     (lr_config.config -> 'long' ->> 'avg_long_order_balance_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' third_long_order_balance_pc` ' ||
                     (lr_config.config -> 'long' ->> 'third_long_order_balance_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' dev_long_pc` ' || lr_config.dev_long_pc || ln;
        --av_config := av_config || '`/set n' || ln_acc_num || ' add_dev_long_pc` ' || lr_config.add_dev_long_pc || ln;
        --av_config := av_config || '`/set n' || ln_acc_num || ' avg_dev_long_pc` ' || lr_config.avg_dev_long_pc || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' add_dev_long_pc` ' ||
                     (lr_config.config -> 'long' ->> 'add_dev_long_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' avg_dev_long_pc` ' ||
                     (lr_config.config -> 'long' ->> 'avg_dev_long_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' third_dev_long_pc` ' ||
                     (lr_config.config -> 'long' ->> 'third_dev_long_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' take_profit_long_pc` ' ||
                     (lr_config.config -> 'long' ->> 'take_profit_long_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' first_take_profit_long_amount_pc` ' ||
                     (lr_config.config -> 'long' ->> 'first_take_profit_long_amount_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' second_take_profit_long_add_pc` ' ||
                     (lr_config.config -> 'long' ->> 'second_take_profit_long_add_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' take_profit_long_to_zero_after_minutes` ' ||
                     (lr_config.config -> 'long' ->> 'take_profit_long_to_zero_after_minutes') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' stop_loss_long_pc` ' ||
                     (lr_config.config -> 'long' ->> 'stop_loss_long_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' stop_loss_long_to_zero_when_profit_pc` ' ||
                     (lr_config.config -> 'long' ->> 'stop_loss_long_to_zero_when_profit_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' trailing_stop_long_callback_pc` ' ||
                     (lr_config.config -> 'long' ->> 'trailing_stop_long_callback_pc') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' cancel_new_long_order_sec` ' ||
                     (lr_config.config -> 'long' ->> 'cancel_new_long_order_sec') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' close_long_position_minutes` ' ||
                     (lr_config.config -> 'long' ->> 'close_long_position_minutes') || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' zero_point_long_pc` ' ||
                     (lr_config.config -> 'long' ->> 'zero_point_long_pc') || ln;

        av_config := av_config || '--------------------------' || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' move_stop_loss_check_ema` ' ||
                     lr_config.move_stop_loss_check_ema || ln;
        av_config := av_config || '`/set n' || ln_acc_num || ' average_start_mode` ' ||
                     (lr_config.config ->> 'average_start_mode') || ln;

        av_config := av_config || '--------------------------' || ln;
        av_config := av_config || '/set n' || ln_acc_num || ' parameter - show available values' || ln;
        av_config := av_config || '/set n' || ln_acc_num || ' parameter value - set parameter';
    END IF;
END;
$function$

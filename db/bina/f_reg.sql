CREATE OR REPLACE FUNCTION bina.f_reg(av_telegram_id IN  VARCHAR,
                                      av_first_name IN   VARCHAR,
                                      av_phone_number IN VARCHAR,
                                      av_error OUT       VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_id                    INTEGER;
    ld_last_deploy_date      TIMESTAMP;
    lj_csv_config            JSON := '{
    "delimeter": ",",
    "number_delimeter": ","
}';
    lj_default_config        JSON := '{
	"long": {
	    "leverage_long": 1,
        "add_dev_long_pc": 0,
        "avg_dev_long_pc": 8,
		"new_long_order_balance_pc": 50,
		"avg_long_order_balance_pc": 98,
        "third_long_order_balance_pc": 0,
        "take_profit_long_pc": 2,
        "first_take_profit_long_amount_pc": 50,
        "second_take_profit_long_add_pc": 10,
	    "third_dev_long_pc": 15,
        "cancel_new_long_order_sec": 20,
	    "close_long_position_minutes": 30,
        "stop_loss_long_pc": 2,
        "stop_loss_long_to_zero_when_profit_pc": 0.5,
        "take_profit_long_to_zero_after_minutes": 0,
        "trailing_stop_long_callback_pc": 2
	},
	"short": {
	    "leverage_short": 1,
        "add_dev_short_pc": 4,
        "avg_dev_short_pc": 8,
		"new_short_order_balance_pc": 50,
		"avg_short_order_balance_pc": 98,
        "third_short_order_balance_pc": 0,
	    "third_dev_short_pc": 15,
        "take_profit_short_pc": 2,
        "first_take_profit_short_amount_pc": 50,
        "second_take_profit_short_add_pc": 10,
        "cancel_new_short_order_sec": 20,
        "close_short_position_minutes": 30,
        "stop_loss_short_pc": 2,
        "stop_loss_short_to_zero_when_profit_pc": 0.5,
        "take_profit_short_to_zero_after_minutes": 0,
        "trailing_stop_short_callback_pc": 2
	},
    "average_start_mode": "P"
}'::JSONB::JSON;

    lj_default_symbol_config JSON := '{
    "long": {
        "block_symbol_long_by_dev": "Y"
    },
    "short": {
        "block_symbol_short_by_dev": "Y"
    },
    "symbol_filter_mode": "N",
    "block_new_symbol_hr": 12,
    "block_symbol_dev_pc": 25,
    "block_symbol_period_hr": 24,
    "block_symbol_avg_price_hr": 0,
    "block_symbol_avg_price_hr2": 0,
    "pause_bot_after_stop_loss_count": 2,
    "block_symbol_after_stop_loss_usdt": 1,
    "block_symbol_after_stop_loss_minutes": 60,
    "block_symbol_after_take_profit_seconds": 0,
    "pause_bot_after_stop_loss_interval_sec": 30
}'::JSONB::JSON;

    lj_default_notification  JSON := '{
    "new_event": true,
    "failed_order": true,
    "created_order": true,
    "blocked_symbol": false,
    "canceled_order": true,
    "opened_position": true,
    "realised_profit": true
}'::JSONB::JSON;
BEGIN

    SELECT s.id
    INTO ln_id
    FROM bina.subscribers s
    WHERE
        s.telegram_id = av_telegram_id;

    IF ln_id > 0 THEN
        av_error := 'You have already registered';
        RETURN;
    END IF;

    /*SELECT MAX(s.last_deploy_date)
    INTO ld_last_deploy_date
    FROM bina.subscribers s;*/

    INSERT INTO
        bina.subscribers (name, phone_number, subscribe_time, status, trading_mode, telegram_id,
                          symbols, ema_index, notifications,
                          binance_leverage, test_mode, /*last_deploy_date, */dev_short_pc, dev_long_pc, move_stop_loss_check_ema,
                          csv_config, config, symbol_config,
                          active_orders, active_positions,
                          acc_num, api_configured, api_validated, approved)
    VALUES
        (av_first_name, av_phone_number, LOCALTIMESTAMP, 'P', 'M', av_telegram_id, NULL,
         1, lj_default_notification,
         1, 'B', 1, 1, /*ld_last_deploy_date,*/ 0,
         lj_csv_config, lj_default_config, lj_default_symbol_config,
         0, 0,
         1, 'N', 'N', 'N');

END;
$function$

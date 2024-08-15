CREATE OR REPLACE FUNCTION bina.f_set_config(av_telegram_id IN VARCHAR,
                                             av_acc_num IN     VARCHAR,
                                             av_parameter IN   VARCHAR,
                                             av_value IN       VARCHAR,
                                             av_error OUT      VARCHAR)
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    g_max_leverage CONSTANT SMALLINT := 5;
    lb_path_found           BOOLEAN  := FALSE;
    lb_found                BOOLEAN  := FALSE;
    ln_acc_num              SMALLINT;
    ln             CONSTANT VARCHAR  := CHR(10);
    ln_value                NUMERIC;
    ln_count                SMALLINT;
    ln_api_updated          INTEGER;
    lv_api_configured       VARCHAR(1);
    cur                     RECORD;
    lv_apikey               VARCHAR;
    lv_secretkey            VARCHAR;
    lv_path                 VARCHAR;
    lb_value                BOOLEAN;
BEGIN
    SET SESSION TIMEZONE TO UTC;

    IF av_acc_num IS NULL THEN
        av_error := 'Account number undefined';
        RETURN;
    END IF;

    BEGIN
        ln_acc_num := av_acc_num::SMALLINT;
    EXCEPTION
        WHEN OTHERS THEN
            av_error := 'Account undefined';
            RETURN;
    END;

    BEGIN
        ln_value := av_value::NUMERIC;
    EXCEPTION
        WHEN OTHERS THEN
    END;

    IF av_value = 'Y' THEN
        lb_value := TRUE;
    ELSIF av_value = 'N' THEN
        lb_value := FALSE;
    END IF;

    IF av_parameter ILIKE '%short%' THEN
        lv_path := 'short';
        lb_path_found := TRUE;
    ELSIF av_parameter ILIKE '%long%' THEN
        lv_path := 'long';
        lb_path_found := TRUE;
    END IF;

    SELECT s.*
    INTO cur
    FROM bina.subscribers s
    WHERE
          s.telegram_id = av_telegram_id
      AND s.acc_num = ln_acc_num;

    IF cur.id IS NULL THEN
        av_error := 'Account not found';
        RETURN;
    END IF;

    lb_found := FALSE;
    IF av_parameter = 'status' THEN
        lb_found := TRUE;
        IF UPPER(av_value) IN ('A', 'P') THEN
            UPDATE bina.subscribers s
            SET
                status = UPPER(av_value)
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln
                            || 'A - active,' || ln || 'P - paused';
        END IF;

    ELSIF av_parameter = 'test_mode' THEN
        lb_found := TRUE;
        IF UPPER(av_value) IN ('S', 'B') THEN
            UPDATE bina.subscribers s
            SET
                test_mode = UPPER(av_value)
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln ||
                        'S - stable (used all balance for trading),' || ln ||
                        'B - beta (used max 30$ for trading)';
        END IF;

    ELSIF av_parameter = 'trading_mode' THEN
        lb_found := TRUE;
        IF UPPER(av_value) IN ('M', 'A', 'O') THEN
            UPDATE bina.subscribers s
            SET
                trading_mode = UPPER(av_value)
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln ||
                        'M - manual,' || ln || 'A - auto';
        END IF;

    ELSIF av_parameter = 'average_start_mode' THEN
        lb_found := TRUE;
        IF UPPER(av_value) IN ('S', 'P', 'C') THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [av_parameter],
                                                UPPER('"' || av_value || '"')::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln ||
                        'S - create average order after initial order created
P - create average order after initial position opened
С - create average order after initial order but correct when position opened by actual entry price';
        END IF;

    ELSIF av_parameter = 'ema_index' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 1 AND 6 THEN
            UPDATE bina.subscribers s
            SET
                ema_index = ln_value
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln ||
                        '1 - prev price,' || ln || '2 - EMA_3,' || ln ||
                        '3 - EMA_5,' || ln || '4 - EMA_10,' || ln ||
                        '5 - EMA_60,' || ln || '6 - EMA_300';
        END IF;

    ELSIF av_parameter LIKE 'new_%_order_balance_pc' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 99 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..99%';
        END IF;

    ELSIF av_parameter LIKE 'avg_%_order_balance_pc' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 99 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..99%' || ln
                || '0 - for disable average order';
        END IF;

    ELSIF av_parameter LIKE 'third_%_order_balance_pc' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 99 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..99%' || ln
                || '0 - for disable third order';
        END IF;

    ELSIF av_parameter LIKE 'take_profit_%_to_zero_after_minutes' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 60 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..60 minutes' || ln
                || '0 - for disable move take_profit order to zero';
        END IF;

    ELSIF av_parameter LIKE 'stop_loss_%_to_zero_when_profit_pc' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 20 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..20%' || ln
                || '0 - for disable move stop_loss order to zero';
        END IF;

    ELSIF av_parameter LIKE 'trailing_stop_%_callback_pc' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 5 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..5% (step 0.1%)' || ln
                            || '0 - for disable trailing_stop order' || ln
                || 'NB. Take profit cancelled when trailing_stop order created';
        END IF;

    ELSIF av_parameter = 'move_stop_loss_check_ema' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 6 THEN
            UPDATE bina.subscribers s
            SET
                move_stop_loss_check_ema = ln_value
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..6' || ln
                || '0 - for disable checking EMA at moving stop_loss to zero';
        END IF;

    ELSIF av_parameter = 'binance_leverage' THEN
        lb_found := TRUE;
        IF ln_value IN (1, 2, 3, 4, 5) THEN
            IF ln_value >= (cur.config -> 'short' ->> 'leverage_short')::NUMERIC THEN
                IF ln_value >= (cur.config -> 'long' ->> 'leverage_long')::NUMERIC THEN
                    UPDATE bina.subscribers s
                    SET
                        binance_leverage = ln_value
                    WHERE
                        s.id = cur.id;
                ELSE
                    av_error := 'Wrong value.' || ln ||
                                'Rule violation: binance_leverage >= leverage_long';
                END IF;
            ELSE
                av_error := 'Wrong value.' || ln ||
                            'Rule violation: binance_leverage >= leverage_short';
            END IF;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values: 1,2,3,4,5' || ln
                || 'It is leverage used on binance side';
        END IF;

    ELSIF av_parameter IN ('leverage_short', 'leverage_long') THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0.1 AND cur.binance_leverage THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0.1..' || cur.binance_leverage;
        END IF;

    ELSIF av_parameter = 'dev_short_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 1 AND 100 THEN
            UPDATE bina.subscribers s
            SET
                dev_short_pc = ln_value
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 1..50 %';
        END IF;

    ELSIF av_parameter = 'dev_long_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 1 AND 100 THEN
            UPDATE bina.subscribers s
            SET
                dev_long_pc = ln_value
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 1..50 %';
        END IF;

    ELSIF av_parameter LIKE 'zero_point_%_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN -1.0 AND 1.0 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values -1.0 .. +1.0%';
        END IF;

    ELSIF av_parameter LIKE 'add_dev_%_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN -100 AND 100 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values -20..20%';
        END IF;

    ELSIF av_parameter LIKE 'avg_dev_%_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 200 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..20%';
        END IF;

        /*ELSIF av_parameter = 'add_dev_short_pc' THEN
            lb_found := TRUE;
            IF ln_value BETWEEN -20 AND 20 THEN
                UPDATE bina.subscribers s
                SET
                    add_dev_short_pc = ln_value
                WHERE
                    s.id = cur.id;
            ELSE
                av_error := 'Wrong value.' || ln || 'Available values -20..20 %';
            END IF;

        ELSIF av_parameter = 'add_dev_long_pc' THEN
            lb_found := TRUE;
            IF ln_value BETWEEN -20 AND 20 THEN
                UPDATE bina.subscribers s
                SET
                    add_dev_long_pc = ln_value
                WHERE
                    s.id = cur.id;
            ELSE
                av_error := 'Wrong value.' || ln || 'Available values -20..20 %';
            END IF;*/

        /*ELSIF av_parameter = 'avg_dev_short_pc' THEN
            lb_found := TRUE;
            IF ln_value BETWEEN 0 AND 20 THEN
                UPDATE bina.subscribers s
                SET
                    avg_dev_short_pc = ln_value
                WHERE
                    s.id = cur.id;
            ELSE
                av_error := 'Wrong value.' || ln || 'Available values 0..20 %';
            END IF;

        ELSIF av_parameter = 'avg_dev_long_pc' THEN
            lb_found := TRUE;
            IF ln_value BETWEEN 0 AND 20 THEN
                UPDATE bina.subscribers s
                SET
                    avg_dev_long_pc = ln_value
                WHERE
                    s.id = cur.id;
            ELSE
                av_error := 'Wrong value.' || ln || 'Available values 0..20 %';
            END IF;*/

    ELSIF av_parameter LIKE 'take_profit_%_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 50 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..50%' || ln
                || '0 - for disable take_profit order';
        END IF;

    ELSIF av_parameter LIKE 'first_take_profit_%_amount_pc' THEN
        lb_found := TRUE;
        IF ln_value IN (10, 20, 30, 40, 50, 60, 70, 80, 90, 100) THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 10..100% (step=10)' || ln
                || '100 - for disable second take_profit order';
        END IF;

    ELSIF av_parameter LIKE 'second_take_profit_%_add_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 100 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..100%' || ln
                || '0 - for disable second take_profit order';
        END IF;

    ELSIF av_parameter LIKE 'stop_loss_%_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 50 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..50%' || ln
                || '0 - for disable stop_loss order';
        END IF;

    ELSIF av_parameter LIKE 'cancel_new_%_order_sec' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 1 AND 1000 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 1..1000 seconds';
        END IF;

    ELSIF av_parameter LIKE 'close_%_position_minutes' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 1000 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..1000 minutes' || ln
                || '0 - for disable close position';
        END IF;

    ELSIF av_parameter LIKE 'third_dev_%_pc' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0 AND 50 THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..50%';
        END IF;

    ELSIF av_parameter LIKE 'third_leverage_%' THEN
        lb_found := lb_path_found;
        IF ln_value BETWEEN 0.1 AND cur.binance_leverage THEN
            UPDATE bina.subscribers s
            SET
                config = JSONB_PRETTY(JSONB_SET(s.config::JSONB, ARRAY [lv_path, av_parameter], ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0.1..' || cur.binance_leverage;
        END IF;

        --------------------------
    ELSIF av_parameter IN
          ('new_event', 'created_order', 'failed_order', 'blocked_symbol', 'canceled_order', 'opened_position',
           'realised_profit') THEN
        lb_found := TRUE;
        IF lb_value IN (TRUE, FALSE) THEN
            UPDATE bina.subscribers s
            SET
                notifications = JSONB_PRETTY(JSONB_SET(s.notifications::JSONB, ARRAY [av_parameter],
                                                       lb_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values +/-';
        END IF;

        --------------------------
    ELSIF av_parameter = 'symbol_filter_mode' THEN
        lb_found := TRUE;
        IF UPPER(av_value) IN ('W', 'B', 'N') THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ('"' || UPPER(av_value) || '"')::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln || 'W - white list,' || ln ||
                        'B - block list,' || ln || 'N - Not used';
        END IF;

    ELSIF av_parameter = 'block_symbol_by_dev' THEN
        lb_found := TRUE;
        IF UPPER(av_value) IN ('Y', 'N') THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ('"' || UPPER(av_value) || '"')::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln || 'Y - apply dynamic blocking' || ln ||
                        'N - no dynamic blocking';
        END IF;

    ELSIF av_parameter ILIKE 'block_symbol_%_by_dev' THEN
        lb_found := TRUE;
        IF UPPER(av_value) IN ('Y', 'N') THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [lv_path, av_parameter],
                                                       ('"' || UPPER(av_value) || '"')::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values:' || ln || 'Y - apply dynamic blocking' || ln ||
                        'N - no dynamic blocking';
        END IF;

    ELSIF av_parameter = 'block_symbol_dev_pc' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 10 AND 99 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 10..99 %';
        END IF;

    ELSIF av_parameter = 'block_symbol_period_hr' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 1 AND 999 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 1..999 hours';
        END IF;

    ELSIF av_parameter = 'block_symbol_avg_price_hr' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 999 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..999 hours,' || ln
                || '0 - no blocking by average price';
        END IF;

    ELSIF av_parameter = 'block_new_symbol_hr' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 2 AND 999 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 2..999 hours';
        END IF;

    ELSIF av_parameter = 'block_symbol_after_take_profit_seconds' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 999 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..999 seconds,' || ln
                || '0 - no blocking after take profit';
        END IF;

    ELSIF av_parameter = 'block_symbol_after_stop_loss_minutes' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 999 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..999 minutes,' || ln
                || '0 - no blocking after stop loss';
        END IF;

    ELSIF av_parameter = 'block_symbol_after_stop_loss_usdt' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 999 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..999 usdt';
        END IF;

    ELSIF av_parameter = 'pause_bot_after_stop_loss_count' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 1 AND 10 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 1..10';
        END IF;

    ELSIF av_parameter = 'pause_bot_after_stop_loss_interval_sec' THEN
        lb_found := TRUE;
        IF ln_value BETWEEN 0 AND 999 THEN
            UPDATE bina.subscribers s
            SET
                symbol_config = JSONB_PRETTY(JSONB_SET(s.symbol_config::JSONB, ARRAY [av_parameter],
                                                       ln_value::TEXT::JSONB))::JSON
            WHERE
                s.id = cur.id;
        ELSE
            av_error := 'Wrong value.' || ln || 'Available values 0..999 seconds,' || ln
                || '0 - no blocking after stop loss';
        END IF;

    ELSIF av_parameter = 'apikey' THEN
        lb_found := TRUE;
        IF LENGTH(av_value) = 64 THEN
            SELECT COUNT(1)
            INTO ln_count
            FROM bina.subscribers s
            WHERE
                  s.binance_acc ->> 'apikey' = av_value
              AND s.id != cur.id;

            IF ln_count > 0 THEN
                av_error := 'ERROR! apikey already used';
                RETURN;
            END IF;

            UPDATE bina.subscribers s
            SET
                --apikey = av_value
                binance_acc = JSONB_PRETTY(JSONB_SET(s.binance_acc::JSONB, ARRAY [av_parameter],
                                                     ('"' || av_value || '"')::JSONB))::JSON
            WHERE
                  s.id = cur.id
              AND (s.binance_acc ->> 'apikey' != av_value OR s.binance_acc ->> 'apikey' IS NULL);
        ELSE
            av_error := 'Wrong apikey';
        END IF;

    ELSIF av_parameter = 'secretkey' THEN
        lb_found := TRUE;
        IF LENGTH(av_value) = 64 THEN
            UPDATE bina.subscribers s
            SET
                --secretkey = av_value
                binance_acc = JSONB_PRETTY(JSONB_SET(s.binance_acc::JSONB, ARRAY [av_parameter],
                                                     ('"' || av_value || '"')::JSONB))::JSON
            WHERE
                  s.id = cur.id
              AND (s.binance_acc ->> 'secretkey' != av_value OR s.binance_acc ->> 'secretkey' IS NULL);
        ELSE
            av_error := 'Wrong apikey';
        END IF;

    ELSIF av_parameter = 'apiremove' THEN
        UPDATE bina.subscribers s
        SET
            binance_acc               = JSON_BUILD_OBJECT('apikey', NULL, 'secretkey', NULL),
            api_configured            = 'N',
            api_validated             = 'N',
            api_validation_error      = NULL,
            balance_usdt              = NULL,
            balance_request_last_date = NULL
        WHERE
            s.id = cur.id;

    END IF;

    IF av_parameter IN ('apikey', 'secretkey') THEN
        UPDATE bina.subscribers s
        SET
            api_configured = CASE
                                 WHEN LENGTH(s.binance_acc ->> 'apikey') = 64
                                     AND LENGTH(s.binance_acc ->> 'secretkey') = 64 THEN 'Y'
                                 ELSE 'N'
                             END
        WHERE
              s.id = cur.id
          AND s.api_configured = 'N'
        RETURNING api_configured
            INTO lv_api_configured;

        GET DIAGNOSTICS ln_api_updated = ROW_COUNT;
        IF ln_api_updated > 0 OR cur.api_validated = 'N' AND lv_api_configured = 'Y' THEN
            av_error := 'api_validation_required';
        END IF;
    END IF;

    IF NOT lb_found THEN
        av_error := 'Unknown parameter';
    END IF;

    IF av_error IS NULL THEN
        UPDATE bina.subscribers s
        SET
            update_time = LOCALTIMESTAMP
        WHERE
            s.id = cur.id;
    END IF;

    PERFORM bina.f_trade_log(an_subscriber_id => cur.id,
                             av_comment => 'set_config',
                             aj_params => JSON_BUILD_OBJECT('telegram_id', av_telegram_id,
                                                            'acc_num', av_acc_num,
                                                            'parameter', av_parameter,
                                                            'value', av_value,
                                                            'error', av_error));

END ;
$function$

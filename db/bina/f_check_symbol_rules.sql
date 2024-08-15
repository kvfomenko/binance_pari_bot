CREATE OR REPLACE FUNCTION bina.f_check_symbol_rules(av_symbol IN        VARCHAR,
                                                     an_price IN         REAL,
                                                     an_cur_dev_pc IN    REAL,
                                                     an_subscriber_id IN INTEGER,
                                                     aj_result OUT       JSON)
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    conf                                      RECORD;
    lb_blocked                                BOOLEAN;
    ln_avg_price                              NUMERIC;
    ln_avg_price2                             NUMERIC;
    lb_no_prev_data                           BOOLEAN;
    ltv_symbols_mask                          VARCHAR(20)[];
    ln_price_close                            NUMERIC;
    ln_take_profit_pc                         NUMERIC;
    ln_deviation_pc                           NUMERIC;
    ln_exist_0                                NUMERIC;
    ln_exist_1                                NUMERIC;
    ln_exist_2                                NUMERIC;
    ln_block_new_symbol_hr                    NUMERIC;
    lv_block_symbol_by_dev                    VARCHAR(1);
    lv_block_symbol_by_dev2                   VARCHAR(1);
    ln_block_symbol_after_stop_loss_minutes   NUMERIC;
    ln_block_symbol_after_take_profit_seconds NUMERIC;
    ltv_reasons                               VARCHAR[];
    g_default_take_profit_pc CONSTANT         NUMERIC := 2;
    g_max_take_profit_pc     CONSTANT         NUMERIC := 3;
    ln_block_symbol_dev_pc                    NUMERIC;
    ln_block_symbol_period_hr                 NUMERIC;
    ln_block_symbol_avg_price_hr              NUMERIC;
    ln_block_symbol_avg_price_hr2             NUMERIC;
BEGIN

    SELECT
        s.symbols, s.symbol_config, s.config
    INTO conf
    FROM bina.subscribers s
    WHERE
        s.id = an_subscriber_id;

    IF conf.symbol_config ->> 'symbol_filter_mode' IN ('B', 'W') AND conf.symbols IS NOT NULL
        AND ARRAY_LENGTH(conf.symbols, 1) > 0 THEN

        SELECT ARRAY_AGG(s || '%')
        INTO ltv_symbols_mask
        FROM UNNEST(conf.symbols) s;

        IF conf.symbol_config ->> 'symbol_filter_mode' = 'W' THEN
            IF av_symbol = ANY (conf.symbols)
                OR av_symbol LIKE ANY (ltv_symbols_mask) THEN
                --ab_valid := TRUE;
                --RETURN; -- white list has more priority then block_symbol_by_dev
            ELSE
                ltv_reasons := ARRAY_APPEND(ltv_reasons, 'blocked.symbol_filter_mode.W'::TEXT);
            END IF;

        ELSIF conf.symbol_config ->> 'symbol_filter_mode' = 'B' THEN
            IF av_symbol = ANY (conf.symbols)
                OR av_symbol LIKE ANY (ltv_symbols_mask) THEN
                ltv_reasons := ARRAY_APPEND(ltv_reasons, 'blocked.symbol_filter_mode.B'::TEXT);
            ELSE
                --ab_valid := TRUE;
            END IF;
        END IF;
    END IF;

    lv_block_symbol_by_dev := conf.symbol_config ->> 'block_symbol_by_dev';
    IF an_cur_dev_pc > 0 THEN
        lv_block_symbol_by_dev2 := conf.symbol_config -> 'short' ->> 'block_symbol_short_by_dev';
    ELSE
        lv_block_symbol_by_dev2 := conf.symbol_config -> 'long' ->> 'block_symbol_long_by_dev';
    END IF;
    IF lv_block_symbol_by_dev2 IS NOT NULL THEN
        lv_block_symbol_by_dev := lv_block_symbol_by_dev2;
    END IF;
    IF lv_block_symbol_by_dev = 'Y' THEN
        ln_block_symbol_dev_pc := (conf.symbol_config ->> 'block_symbol_dev_pc')::NUMERIC;
        ln_block_symbol_period_hr := (conf.symbol_config ->> 'block_symbol_period_hr')::NUMERIC;

        SELECT MAX(s.max_price) / MIN(s.min_price) * 100 - 100
        INTO ln_deviation_pc
        FROM bina.rates_stat s
        WHERE
              s.symbol = av_symbol
          AND s.price_date > LOCALTIMESTAMP - (ln_block_symbol_period_hr || ' hour')::INTERVAL;

        IF ln_deviation_pc >= ln_block_symbol_dev_pc THEN
            ltv_reasons := ARRAY_APPEND(ltv_reasons, 'blocked.block_symbol_by_dev:' || ROUND(ln_deviation_pc, 3)::TEXT);
        END IF;

        ln_block_new_symbol_hr = (conf.symbol_config ->> 'block_new_symbol_hr')::NUMERIC;

        SELECT
            1 AS exist_0,
            (SELECT COUNT(1)
             FROM bina.rates_stat s0
             WHERE
                   s0.symbol = rs.symbol
               AND s0.price_date =
                   DATE_TRUNC('hour', LOCALTIMESTAMP - (ln_block_new_symbol_hr || ' hour')::INTERVAL)) AS exist_1,
            -- additional check: if this symbol was week before or not
            (SELECT COUNT(1)
             FROM bina.rates_stat s0
             WHERE
                   s0.symbol = rs.symbol
               AND s0.price_date = DATE_TRUNC('hour', LOCALTIMESTAMP - (ln_block_new_symbol_hr || ' hour')::INTERVAL) -
                                   INTERVAL '7 days') AS exist_2
        INTO ln_exist_0, ln_exist_1, ln_exist_2
        FROM bina.rates_stat rs
        WHERE
              rs.symbol = av_symbol
          AND rs.price_date >= DATE_TRUNC('hour', LOCALTIMESTAMP) - INTERVAL '2 hour';

        /*SELECT TRUE
        INTO lb_no_prev_data
        FROM bina.rates_stat rs
        WHERE
              rs.symbol = av_symbol
          AND rs.price_date >= DATE_TRUNC('hour', LOCALTIMESTAMP) - INTERVAL '1 hour'
          AND NOT EXISTS (SELECT 1
                          FROM bina.rates_stat s0
                          WHERE
                                s0.symbol = rs.symbol
                            AND s0.price_date IN (
                                                  DATE_TRUNC('hour',
                                                             LOCALTIMESTAMP -
                                                             (ln_block_new_symbol_hr || ' hour')::INTERVAL),
                    -- additional check: if this symbol was week before or not
                                                  DATE_TRUNC('hour', LOCALTIMESTAMP
                                                      - (ln_block_new_symbol_hr || ' hour')::INTERVAL)
                                                      - INTERVAL '7 days'));*/

        IF COALESCE(ln_exist_0, 0) = 0 OR (ln_exist_1 = 0 AND ln_exist_2 = 0) THEN
            -- if current data exists and no prev data exists then IT IS NEW SYMBOL SHOULD BE BLOCKED
            ltv_reasons := ARRAY_APPEND(ltv_reasons,
                                        'blocked.new_symbol_period:'::TEXT || COALESCE(ln_exist_0, 0) || '_' ||
                                        COALESCE(ln_exist_1, 0) || '_' || COALESCE(ln_exist_2, 0));
        END IF;
    END IF;

    ln_block_symbol_avg_price_hr := (conf.symbol_config ->> 'block_symbol_avg_price_hr')::NUMERIC;
    IF ln_block_symbol_avg_price_hr > 0 THEN
        ln_block_symbol_avg_price_hr2 := (conf.symbol_config ->> 'block_symbol_avg_price_hr2')::NUMERIC;

        SELECT AVG(s.max_price)
        INTO ln_avg_price
        FROM bina.rates_stat s
        WHERE
              s.symbol = av_symbol
          AND s.price_date > LOCALTIMESTAMP - (ln_block_symbol_avg_price_hr || ' hours')::INTERVAL;

        IF ln_block_symbol_avg_price_hr2 > 0 THEN
            SELECT AVG(s.max_price)
            INTO ln_avg_price2
            FROM bina.rates_stat s
            WHERE
                  s.symbol = av_symbol
              AND s.price_date > LOCALTIMESTAMP - (ln_block_symbol_avg_price_hr2 || ' hours')::INTERVAL;
        END IF;

        IF ln_avg_price IS NULL THEN
            ltv_reasons := ARRAY_APPEND(ltv_reasons, 'blocked.block_symbol_opposite_side_no_data');
        END IF;

        IF an_cur_dev_pc > 0 THEN
            ln_take_profit_pc := (conf.config -> 'short' ->> 'take_profit_short_pc')::NUMERIC;
            IF ln_take_profit_pc = 0 THEN ln_take_profit_pc := g_default_take_profit_pc; END IF;
            IF ln_take_profit_pc > g_max_take_profit_pc THEN
                ln_take_profit_pc := g_max_take_profit_pc;
            END IF;
            ln_price_close = an_price * (1 - ln_take_profit_pc / 100);
            IF ln_price_close < ln_avg_price THEN
                ltv_reasons := ARRAY_APPEND(ltv_reasons,
                                            'blocked.block_symbol_opposite_side:' || ROUND(ln_avg_price, 8)::TEXT);
            ELSIF ln_price_close < ln_avg_price2 THEN
                ltv_reasons := ARRAY_APPEND(ltv_reasons,
                                            'blocked.block_symbol_opposite_side2:' ||
                                            ROUND(ln_avg_price2, 8)::TEXT);
            END IF;
        ELSE
            ln_take_profit_pc := (conf.config -> 'long' ->> 'take_profit_long_pc')::NUMERIC;
            IF ln_take_profit_pc = 0 THEN ln_take_profit_pc := g_default_take_profit_pc; END IF;
            IF ln_take_profit_pc > g_max_take_profit_pc THEN
                ln_take_profit_pc := g_max_take_profit_pc;
            END IF;
            ln_price_close = an_price * (1 + ln_take_profit_pc / 100);
            IF ln_price_close > ln_avg_price THEN
                ltv_reasons := ARRAY_APPEND(ltv_reasons,
                                            'blocked.block_symbol_opposite_side:' ||
                                            ROUND(ln_avg_price, 8)::TEXT);
            ELSIF ln_price_close > ln_avg_price2 THEN
                ltv_reasons := ARRAY_APPEND(ltv_reasons,
                                            'blocked.block_symbol_opposite_side2:' ||
                                            ROUND(ln_avg_price2, 8)::TEXT);
            END IF;
        END IF;
    END IF;

    ln_block_symbol_after_stop_loss_minutes :=
            (conf.symbol_config ->> 'block_symbol_after_stop_loss_minutes')::NUMERIC;
    IF ln_block_symbol_after_stop_loss_minutes > 0 THEN
        SELECT TRUE
        INTO lb_blocked
        FROM bina.trade_log x
        WHERE
              x.subscriber_id = an_subscriber_id
          AND x.log_date >=
              LOCALTIMESTAMP - (ln_block_symbol_after_stop_loss_minutes || ' minutes')::INTERVAL
          AND x.comment = 'filled_order_stop_loss'
          AND x.params ->> 'symbol' = av_symbol
          AND (x.params ->> 'filled_amount')::NUMERIC <=
              -COALESCE((conf.symbol_config ->> 'block_symbol_after_stop_loss_usdt')::NUMERIC, 1)
        LIMIT 1;

        IF lb_blocked THEN
            ltv_reasons := ARRAY_APPEND(ltv_reasons, 'blocked.block_symbol_after_stop_loss'::TEXT);
        END IF;
    END IF;

    ln_block_symbol_after_take_profit_seconds :=
            (conf.symbol_config ->> 'block_symbol_after_take_profit_seconds')::NUMERIC;
    IF ln_block_symbol_after_take_profit_seconds > 0 THEN
        SELECT TRUE
        INTO lb_blocked
        FROM bina.trade_log x
        WHERE
              x.subscriber_id = an_subscriber_id
          AND x.log_date >=
              LOCALTIMESTAMP - (ln_block_symbol_after_take_profit_seconds || ' seconds')::INTERVAL
          AND x.comment = 'filled_order_take_profit'
          AND x.params ->> 'symbol' = av_symbol
        LIMIT 1;

        IF lb_blocked THEN
            ltv_reasons := ARRAY_APPEND(ltv_reasons, 'blocked.block_symbol_after_take_profit'::TEXT);
        END IF;
    END IF;

    IF ARRAY_LENGTH(ltv_reasons, 1) > 0 THEN
        aj_result := JSON_BUILD_OBJECT('validated', FALSE, 'reasons', ltv_reasons);
    ELSE
        aj_result := JSON_BUILD_OBJECT('validated', TRUE);
    END IF;
END;
$function$

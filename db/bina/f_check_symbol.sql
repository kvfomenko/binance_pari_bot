DROP FUNCTION if exists bina.f_check_symbol(character varying,real,real,integer);

CREATE OR REPLACE FUNCTION bina.f_check_symbol(av_symbol IN        VARCHAR,
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
    conf             RECORD;
    lb_blocked       BOOLEAN;
    ln_avg_price     REAL;
    lb_no_prev_data  BOOLEAN;
    ltv_symbols_mask VARCHAR(20)[];
    ln_price_close   REAL;
    ltv_reasons      VARCHAR[];
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
                ltv_reasons := ltv_reasons || 'blocked.symbol_filter_mode.W'::TEXT;
            END IF;

        ELSIF conf.symbol_config ->> 'symbol_filter_mode' = 'B' THEN
            IF av_symbol = ANY (conf.symbols) THEN
                ltv_reasons := ltv_reasons || 'blocked.symbol_filter_mode.B'::TEXT;
            ELSE
                --ab_valid := TRUE;
            END IF;
        END IF;
    END IF;

    IF conf.symbol_config ->> 'block_symbol_by_dev' = 'Y' THEN
        SELECT TRUE
        INTO lb_blocked
        FROM bina.rates_stat s
        WHERE
              s.symbol = av_symbol
          AND s.price_date > LOCALTIMESTAMP - ((conf.symbol_config ->> 'block_symbol_period_hr') || ' hour')::INTERVAL
        GROUP BY
            s.symbol
        HAVING
                            MAX(s.max_price) / MIN(s.min_price) * 100 - 100 >
                            (conf.symbol_config ->> 'block_symbol_dev_pc')::NUMERIC; -- deviation > 25%

        IF lb_blocked THEN
            ltv_reasons := ltv_reasons || 'blocked.block_symbol_by_dev'::TEXT;
        END IF;

        SELECT TRUE
        INTO lb_no_prev_data
        FROM bina.rates_stat rs
        WHERE
              rs.symbol = av_symbol
          AND rs.price_date >= DATE_TRUNC('hour', LOCALTIMESTAMP) - INTERVAL '1 hour'
          AND NOT EXISTS (SELECT 1
                          FROM bina.rates_stat s0
                          WHERE
                                s0.symbol = rs.symbol
                            AND s0.price_date =
                                DATE_TRUNC('hour', LOCALTIMESTAMP -
                                                   ((conf.symbol_config ->> 'block_new_symbol_hr') || ' hour')::INTERVAL));

        IF lb_no_prev_data THEN
            -- if current data exists and no prev data exists then IT IS NEW SYMBOL SHOULD BE BLOCKED
            ltv_reasons := ltv_reasons || 'blocked.new_symbol_period'::TEXT;
        END IF;
    END IF;

    IF (conf.symbol_config ->> 'block_symbol_avg_price_hr')::NUMERIC > 0 THEN
        SELECT AVG(s.max_price)
        INTO ln_avg_price
        FROM bina.rates_stat s
        WHERE
              s.symbol = av_symbol
          AND s.price_date >
              LOCALTIMESTAMP - ((conf.symbol_config ->> 'block_symbol_avg_price_hr') || ' hours')::INTERVAL;

        IF ln_avg_price IS NULL THEN
            ltv_reasons := ltv_reasons || 'blocked.block_symbol_opposite_side_no_data'::TEXT;
        END IF;

        IF an_cur_dev_pc > 0 THEN
            ln_price_close = an_price * (1 - (conf.config -> 'short' ->> 'take_profit_short_pc')::NUMERIC / 100);
            IF ln_price_close < ln_avg_price THEN
                ltv_reasons := ltv_reasons || 'blocked.block_symbol_opposite_side'::TEXT;
            END IF;
        ELSE
            ln_price_close = an_price * (1 + (conf.config -> 'long' ->> 'take_profit_long_pc')::NUMERIC / 100);
            IF ln_price_close > ln_avg_price THEN
                ltv_reasons := ltv_reasons || 'blocked.block_symbol_opposite_side'::TEXT;
            END IF;
        END IF;
    END IF;

    IF (conf.symbol_config ->> 'block_symbol_after_stop_loss_minutes')::NUMERIC > 0 THEN
        SELECT TRUE
        INTO lb_blocked
        FROM bina.trade_log x
        WHERE
              x.subscriber_id = an_subscriber_id
          AND x.log_date >=
              LOCALTIMESTAMP - ((conf.symbol_config ->> 'block_symbol_after_stop_loss_minutes') || ' minutes')::INTERVAL
          AND x.comment = 'filled_order_stop_loss'
          AND x.params ->> 'symbol' = av_symbol
          AND (x.params ->> 'filled_amount')::NUMERIC <=
              COALESCE((conf.symbol_config ->> 'block_symbol_after_stop_loss_usdt')::NUMERIC, 1)
        LIMIT 1;

        IF lb_blocked THEN
            ltv_reasons := ltv_reasons || 'blocked.block_symbol_after_stop_loss'::TEXT;
        END IF;
    END IF;

    IF ARRAY_LENGTH(ltv_reasons, 1) > 0 THEN
        aj_result := JSON_BUILD_OBJECT('validated', FALSE, 'reasons', ltv_reasons);
    ELSE
        aj_result := JSON_BUILD_OBJECT('validated', TRUE);
    END IF;
END;
$function$

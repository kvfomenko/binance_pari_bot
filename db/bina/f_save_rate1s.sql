CREATE OR REPLACE FUNCTION bina.f_save_rate1s(aj_rates              JSON[],
                                              ad_cur_price_date OUT TIMESTAMP)
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    gn_event_threshold_pc CONSTANT SMALLINT := 3;
    ld_now_ms                      TIMESTAMP;
    ld_now                         TIMESTAMP;
    cur                            RECORD;
    lr_prev                        RECORD;
    lr_prev1                       RECORD;
    ld_last_date                   TIMESTAMP;
    ln_price                       REAL;
    ld_price_date                  TIMESTAMP;
    ltn_ema                        REAL[];
    ltn_dev_pc                     REAL[];
    ln_event_id                    SMALLINT;
    lb_new_second                  BOOLEAN  := FALSE;

    st_normal             CONSTANT SMALLINT := 1;
    st_extrapolated       CONSTANT SMALLINT := 2;
BEGIN
    SET SESSION TIMEZONE TO UTC;
    ld_now_ms := LOCALTIMESTAMP;
    ld_now := DATE_TRUNC('second', ld_now_ms);

    FOR cur IN (SELECT
                    x ->> 's' AS symbol,
                    TO_TIMESTAMP(util.f_parse_bigint(x ->> 'e')::NUMERIC / 1000) AS binance_price_date,
                    (x ->> 'p')::REAL AS last_price
                FROM UNNEST(aj_rates) x)
        LOOP
            lb_new_second := FALSE;
            ltn_ema := NULL;
            ltn_dev_pc := NULL;
            ln_event_id := NULL;
            --perform bina.f_log(ld_now, 'ld_now:' || ld_now || ', ' || lv_symbol);

            SELECT r.*
            INTO lr_prev
            FROM bina.rates1s_last r
            WHERE
                r.symbol = cur.symbol;
            --perform bina.f_log(ld_now, 'lr_prev.price:' || cur.symbol || ', ' || lr_prev.price_date ||', '|| lr_prev.price);
            --RAISE INFO 'lr_prev: %, %, %', cur.symbol, lr_prev.price_date, lr_prev.price;

            IF lr_prev.price IS NULL
                OR lr_prev.price_date < ld_now - INTERVAL '1 seconds' THEN
                ld_last_date := DATE_TRUNC('second', lr_prev.price_date);

                IF ld_last_date IS NOT NULL
                    AND ld_last_date > LOCALTIMESTAMP - INTERVAL '5 minutes' THEN
                    -- rebuild missed rows: max last 5 minutes

                    lr_prev1 := lr_prev;
                    -- fill missed range with extrapolated data
                    FOR i IN 1..EXTRACT(EPOCH FROM (ld_now - INTERVAL '1 second') - ld_last_date)
                        LOOP
                            ld_price_date := ld_last_date + (i || ' second')::INTERVAL;
                            ln_price := bina.f_get_price_for_time(ld_last_date, lr_prev.price,
                                                                  ld_now, cur.last_price,
                                                                  ld_price_date);
                            --RAISE INFO 'ld_price_date %=%, %', i, ld_price_date, ln_price;
                            IF ld_price_date < ld_now THEN
                                ltn_ema[1] := lr_prev1.price;
                                ltn_ema[2] := 2::REAL / (3 + 1) * lr_prev1.price +
                                              ((1 - 2::REAL / (3 + 1)) * COALESCE(lr_prev1.ema[2], lr_prev1.price));
                                ltn_ema[3] := 2::REAL / (5 + 1) * lr_prev1.price +
                                              ((1 - 2::REAL / (5 + 1)) * COALESCE(lr_prev1.ema[3], lr_prev1.price));
                                ltn_ema[4] := 2::REAL / (10 + 1) * lr_prev1.price +
                                              ((1 - 2::REAL / (10 + 1)) * COALESCE(lr_prev1.ema[4], lr_prev1.price));
                                ltn_ema[5] := 2::REAL / (60 + 1) * lr_prev1.price +
                                              ((1 - 2::REAL / (60 + 1)) * COALESCE(lr_prev1.ema[5], lr_prev1.price));
                                ltn_ema[6] := 2::REAL / (300 + 1) * lr_prev1.price +
                                              ((1 - 2::REAL / (300 + 1)) * COALESCE(lr_prev1.ema[6], lr_prev1.price));

                                -- perform bina.f_log(ld_now, 'insert st_extrapolated:' || ld_price_date || ',' || ln_price);
                                BEGIN
                                    INSERT INTO
                                        bina.rates (symbol, price_date, status, price, ema, min_price, max_price)
                                    VALUES
                                        (cur.symbol,
                                         ld_price_date,
                                         st_extrapolated,
                                         ln_price,
                                         ltn_ema,
                                         lr_prev.min_price,
                                         lr_prev.max_price)
                                    RETURNING * INTO lr_prev1;
                                EXCEPTION
                                    WHEN OTHERS THEN
                                        NULL;
                                END;
                            END IF;
                        END LOOP;
                END IF;
            END IF;

            --perform bina.f_log(ld_now, 'lr_prev.price2:' || lr_prev.price);

            IF lr_prev.price IS NOT NULL THEN
                ltn_ema[1] := lr_prev.price;
                ltn_ema[2] := 2::REAL / (3 + 1) * lr_prev.price +
                              ((1 - 2::REAL / (3 + 1)) * COALESCE(lr_prev.ema[2], lr_prev.price));
                ltn_ema[3] := 2::REAL / (5 + 1) * lr_prev.price +
                              ((1 - 2::REAL / (5 + 1)) * COALESCE(lr_prev.ema[3], lr_prev.price));
                ltn_ema[4] := 2::REAL / (10 + 1) * lr_prev.price +
                              ((1 - 2::REAL / (10 + 1)) * COALESCE(lr_prev.ema[4], lr_prev.price));
                ltn_ema[5] := 2::REAL / (60 + 1) * lr_prev.price +
                              ((1 - 2::REAL / (60 + 1)) * COALESCE(lr_prev.ema[5], lr_prev.price));
                ltn_ema[6] := 2::REAL / (300 + 1) * lr_prev.price +
                              ((1 - 2::REAL / (300 + 1)) * COALESCE(lr_prev.ema[6], lr_prev.price));

                ln_event_id = NULL;
                FOR i IN 1..6 LOOP
                    ltn_dev_pc[i] := ROUND((cur.last_price / ltn_ema[i] * 100 - 100)::NUMERIC, 2);
                    IF ltn_dev_pc[i] >= gn_event_threshold_pc OR ltn_dev_pc[i] <= -gn_event_threshold_pc THEN
                        ln_event_id = SIGN(ltn_dev_pc[i]);
                    END IF;
                    --RAISE INFO 'dev_pc: %, %, %, %', cur.symbol, ltn_ema[i], ltn_dev_pc[i], ln_event_id;
                END LOOP;
            END IF;
            --perform bina.f_log(ld_now, 'insert:' || ld_now || ' ' || ln_last_price);

            -- save to history when second was changed
            IF DATE_TRUNC('second', lr_prev.price_date) != ld_now THEN
                lb_new_second := TRUE;
                BEGIN
                    INSERT INTO
                        bina.rates (symbol, price_date, status, price, ema, event_id, min_price, max_price)
                    VALUES
                        (cur.symbol, DATE_TRUNC('second', lr_prev.price_date), st_normal,
                         lr_prev.price, lr_prev.ema, lr_prev.event_id,
                         lr_prev.min_price, lr_prev.max_price);
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
            END IF;

            INSERT INTO
                bina.rates1s_last (symbol, price_date, price, ema, event_id, dev_pc, binance_price_date)
            VALUES
                (cur.symbol, ld_now_ms, cur.last_price, ltn_ema, ln_event_id, ltn_dev_pc, cur.binance_price_date)
            ON CONFLICT (symbol)
                DO UPDATE SET
                              price_date         = excluded.price_date,
                              price              = excluded.price,
                              min_price          = CASE
                                                       WHEN lb_new_second THEN excluded.price
                                                       ELSE LEAST(bina.rates1s_last.min_price, excluded.price)
                                                   END,
                              max_price          = CASE
                                                       WHEN lb_new_second THEN excluded.price
                                                       ELSE GREATEST(bina.rates1s_last.max_price, excluded.price)
                                                   END,
                              ema                = excluded.ema,
                              event_id           = CASE
                                                       WHEN lb_new_second THEN excluded.event_id
                                                       ELSE COALESCE(bina.rates1s_last.event_id, excluded.event_id)
                                                   END,
                              dev_pc             = excluded.dev_pc,
                              binance_price_date = excluded.binance_price_date;
        END LOOP;

    ad_cur_price_date := ld_now;

END ;
$function$

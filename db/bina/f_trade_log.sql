CREATE OR REPLACE FUNCTION bina.f_trade_log(an_subscriber_id IN INTEGER,
                                            av_comment IN       VARCHAR,
                                            aj_params IN        JSON,
                                            av_event OUT        VARCHAR)
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    lj_params                                JSON := aj_params;
    l_pause_bot_after_stop_loss_count        INTEGER;
    l_pause_bot_after_stop_loss_interval_sec INTEGER;
    l_block_symbol_after_stop_loss_usdt      NUMERIC;
    l_count_stop_loss                        INTEGER;
BEGIN
    SET SESSION TIMEZONE TO UTC;

    IF av_comment ILIKE '%order%' THEN
        lj_params := JSONB_PRETTY(lj_params::JSONB)::JSON;
    END IF;

    INSERT INTO
        bina.trade_log (log_date, subscriber_id, comment, params)
    VALUES
        (LOCALTIMESTAMP, an_subscriber_id, av_comment, lj_params);

    IF av_comment = 'start_bot' THEN
        UPDATE bina.process p
        SET last_bot_restart_date = LOCALTIMESTAMP;
    END IF;

    IF av_comment = 'filled_order_stop_loss' THEN
        SELECT
            s.symbol_config ->> 'pause_bot_after_stop_loss_count',
            s.symbol_config ->> 'pause_bot_after_stop_loss_interval_sec',
            s.symbol_config ->> 'block_symbol_after_stop_loss_usdt'
        INTO l_pause_bot_after_stop_loss_count,
            l_pause_bot_after_stop_loss_interval_sec,
            l_block_symbol_after_stop_loss_usdt
        FROM bina.subscribers s
        WHERE
            s.id = an_subscriber_id;

        IF l_pause_bot_after_stop_loss_interval_sec > 0 THEN
            SELECT COUNT(1)
            INTO l_count_stop_loss
            FROM bina.trade_log x
            WHERE
                  x.subscriber_id = an_subscriber_id
              AND x.log_date >=
                  LOCALTIMESTAMP - (l_pause_bot_after_stop_loss_interval_sec || ' seconds')::INTERVAL
              AND x.comment = 'filled_order_stop_loss'
              AND (x.params ->> 'filled_amount')::NUMERIC <= -l_block_symbol_after_stop_loss_usdt;

            IF l_count_stop_loss >= l_pause_bot_after_stop_loss_count THEN
                UPDATE bina.subscribers s
                SET status = 'P'
                WHERE
                    s.id = an_subscriber_id;

                av_event := 'bot_paused';

                INSERT INTO
                    bina.trade_log (log_date, subscriber_id, comment, params)
                VALUES
                    (LOCALTIMESTAMP, an_subscriber_id, av_event,
                     JSON_BUILD_OBJECT('count_stop_loss', l_count_stop_loss,
                                       'pause_bot_after_stop_loss_count', l_pause_bot_after_stop_loss_count,
                                       'pause_bot_after_stop_loss_interval_sec',
                                       l_pause_bot_after_stop_loss_interval_sec,
                                       'block_symbol_after_stop_loss_usdt', l_block_symbol_after_stop_loss_usdt));
            END IF;
        END IF;
    END IF;

END;
$function$

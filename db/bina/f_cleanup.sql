CREATE OR REPLACE FUNCTION bina.f_cleanup()
    RETURNS VOID
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    li_save_fresh_data        CONSTANT INTERVAL := INTERVAL '72 hour';
    li_save_data_before_event CONSTANT INTERVAL := INTERVAL '10 minutes';
    li_save_data_after_event  CONSTANT INTERVAL := INTERVAL '120 minutes';
    ld_cleanup_threshold_from          TIMESTAMP;
    ld_cleanup_threshold_to            TIMESTAMP;
BEGIN
    -- process executed every hour
    SET SESSION TIMEZONE TO UTC;

    SELECT p.cleanup_threshold
    INTO ld_cleanup_threshold_from
    FROM bina.process p;

    ld_cleanup_threshold_to := DATE_TRUNC('hour', LOCALTIMESTAMP) - li_save_fresh_data;
    IF ld_cleanup_threshold_to > ld_cleanup_threshold_from THEN

        -- cleanup old rates without events
        DELETE
        FROM bina.rates rr
        WHERE
              rr.price_date BETWEEN ld_cleanup_threshold_from AND ld_cleanup_threshold_to
          AND NOT EXISTS (SELECT 1
                          FROM bina.rates r
                          WHERE
                                r.event_id <> 0
                            AND r.symbol = rr.symbol
                            AND r.price_date BETWEEN rr.price_date - li_save_data_before_event
                                    AND rr.price_date + li_save_data_after_event);

        /*
        SELECT (date_trunc('month', localtimestamp) + interval '1 month' - interval '1 day')::date AS end_of_month;

        create table bina.rates_2025_01
         partition of bina.rates FOR VALUES FROM ('2025-01-01') TO ('2025-01-31 23:59:59')
         with (fillfactor = 99);
        */

        /*IF ld_cleanup_threshold_to::DATE > ld_cleanup_threshold_from::DATE THEN
            -- once a day
            drop partition bina.rates_2023_01;
        END IF;*/

        --PERFORM bina.f_trade_log(an_subscriber_id => 0, av_comment => 'cleanup', aj_params => NULL);

        UPDATE bina.process p
        SET
            cleanup_threshold = ld_cleanup_threshold_to;

    END IF;


END;
$function$

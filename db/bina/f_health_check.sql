CREATE OR REPLACE FUNCTION bina.f_health_check(av_error OUT VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ld_now               TIMESTAMP;
    ln_rows_count        INTEGER;
    ld_last_process_time TIMESTAMP;
BEGIN
    SET SESSION TIMEZONE TO UTC;
    ld_now := LOCALTIMESTAMP;

    SELECT COUNT(1)
    INTO ln_rows_count
    FROM bina.rates1s_last l
    WHERE
        l.price_date >= ld_now - INTERVAL '15 seconds';

    SELECT last_process_time
    INTO ld_last_process_time
    FROM bina.process;

    IF ln_rows_count = 0 OR ld_last_process_time < ld_now - INTERVAL '15 seconds' THEN
        av_error := 'no_rate_loader';
    END IF;
END;
$function$

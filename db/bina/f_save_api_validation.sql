CREATE OR REPLACE FUNCTION bina.f_save_api_validation(an_subscriber_id IN        INTEGER,
                                                      an_balance_usdt IN         NUMERIC,
                                                      av_api_validation_error IN VARCHAR,
                                                      av_error OUT               VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    lv_api_validated                VARCHAR(1);
    g_minimal_balance_usdt CONSTANT NUMERIC := 10;
BEGIN
    SET SESSION TIMEZONE TO UTC;

    IF an_balance_usdt >= g_minimal_balance_usdt THEN
        lv_api_validated := 'Y';
        av_error := 'API validation success';
    ELSE
        lv_api_validated := 'N';
        IF an_balance_usdt IS NULL THEN
            av_error := 'API validation error: ' || av_api_validation_error;
        ELSIF an_balance_usdt < g_minimal_balance_usdt THEN
            av_error := 'API ok, but balance less then ' || g_minimal_balance_usdt || ' USDT, please fund';
        END IF;
    END IF;

    UPDATE bina.subscribers s
    SET
        balance_usdt              = an_balance_usdt,
        balance_request_last_date = LOCALTIMESTAMP,
        api_validation_error      = SUBSTR(av_error, 1, 100),
        api_validated             = lv_api_validated
    WHERE
        s.id = an_subscriber_id;

END;
$function$

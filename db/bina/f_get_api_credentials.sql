CREATE OR REPLACE FUNCTION bina.f_get_api_credentials(av_telegram_id IN   VARCHAR,
                                                      atj_credentials OUT JSON[])
    RETURNS JSON[]
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ld_max_revalidation_dt                        TIMESTAMP;
    ln_api_revalidation_interval_minutes CONSTANT INTEGER := 5;
BEGIN
    SET SESSION TIMEZONE TO UTC;

    ld_max_revalidation_dt := LOCALTIMESTAMP - (ln_api_revalidation_interval_minutes || ' minutes')::INTERVAL;

    SELECT
        ARRAY_AGG(JSON_BUILD_OBJECT('subscriber_id', x.id,
                                    'acc_num', x.acc_num,
                                    'api_validated', x.api_validated,
                                    'api_validation_allowed',
                                    CASE
                                        WHEN x.api_validated = 'Y'
                                            OR x.balance_request_last_date > ld_max_revalidation_dt
                                            THEN 'N'
                                        ELSE 'Y'
                                    END,
                                    'binance_acc', x.binance_acc))
    INTO atj_credentials
    FROM (SELECT s.*
          FROM bina.subscribers s
          WHERE
                s.telegram_id = av_telegram_id
            AND s.api_configured = 'Y'
          ORDER BY
              s.acc_num) x;
END;
$function$

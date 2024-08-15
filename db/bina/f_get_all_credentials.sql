CREATE OR REPLACE FUNCTION bina.f_get_all_credentials(atj_credentials OUT JSON[])
    RETURNS JSON[]
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
BEGIN
    SET SESSION TIMEZONE TO UTC;

    SELECT
        ARRAY_AGG(JSON_BUILD_OBJECT('subscriber_id', x.id,
                                    'telegram_id', x.telegram_id,
                                    'acc_num', x.acc_num,
                                    'binance_acc', x.binance_acc))
    INTO atj_credentials
    FROM (SELECT s.*
          FROM bina.subscribers s
          WHERE
                s.status = 'A'
            AND s.api_configured = 'Y'
            AND s.api_validated = 'Y'
            AND s.approved = 'Y'
          ORDER BY
              s.id) x;
END;
$function$

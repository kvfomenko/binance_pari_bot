CREATE OR REPLACE FUNCTION bina.f_get_api(av_telegram_id IN VARCHAR,
                                          av_config OUT     VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    cur         RECORD;
    ln CONSTANT VARCHAR := CHR(10);
    lv_approved VARCHAR;
BEGIN

    SELECT s.approved
    INTO lv_approved
    FROM bina.subscribers s
    WHERE
          s.telegram_id = av_telegram_id
      AND s.acc_num = 1;

    IF lv_approved IS NULL THEN
        av_config := 'Account not registered' || ln;
        av_config := av_config || 'For registration please send command /start';
        RETURN;
    END IF;

    IF lv_approved = 'Y' THEN
        av_config := 'APIs' || ln;
    ELSE
        av_config := 'APIs' || ' NOT APPROVED' || ln;
    END IF;

    FOR cur IN (SELECT
                    s.acc_num, SUBSTR(s.binance_acc ->> 'apikey', 1, 10) AS apikey,
                    CASE
                        WHEN s.binance_acc ->> 'secretkey' IS NULL THEN 'undefined'
                        ELSE 'configured'
                    END AS secretkey,
                    s.api_configured,
                    s.api_validated,
                    s.api_validation_error
                FROM bina.subscribers s
                WHERE
                    s.telegram_id = av_telegram_id
                ORDER BY
                    s.acc_num)
        LOOP
            av_config := av_config || '--------------------------' || ln;
            av_config := av_config
                             || 'account: n' || cur.acc_num || ln
                             || 'api configured: ' || cur.api_configured || ln
                             || 'api validated: ' || cur.api_validated || ln
                             || 'apikey: ' || coalesce(cur.apikey,'') || ln
                             || 'secretkey: ' || coalesce(cur.secretkey,'') || ln;
            IF COALESCE(cur.api_validation_error, '') != '' THEN
                av_config := av_config || 'api validation error: ' || cur.api_validation_error || ln;
            END IF;
        END LOOP;

    av_config := av_config || '--------------------------' || ln;
    av_config := av_config || 'available commands:' || ln;
    av_config := av_config || '`/set n1 apikey` apikey - setup Binance apikey' || ln;
    av_config := av_config || '`/set n1 secretkey` secretkey - setup Binance secretkey' || ln;
    av_config := av_config || '`/add n1` - add new account with config copied from account n1' || ln;
    av_config := av_config || '`/del n1` - remove account n1' || ln;

    --av_config := av_config || '</pre>';

END;
$function$

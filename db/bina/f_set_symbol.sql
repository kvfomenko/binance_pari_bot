CREATE OR REPLACE FUNCTION bina.f_set_symbol(av_telegram_id IN VARCHAR,
                                             av_acc_num IN     VARCHAR,
                                             av_symbol IN      VARCHAR,
                                             av_operation IN   VARCHAR,
                                             av_error OUT      VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_acc_num       SMALLINT;
    ln_subscriber_id INTEGER;
    ltv_symbols      VARCHAR[];
    ltv_symbols_new  VARCHAR[];
BEGIN
    BEGIN
        ln_acc_num := av_acc_num::SMALLINT;
    EXCEPTION
        WHEN OTHERS THEN
            av_error := 'Account undefined';
            RETURN;
    END;

    SELECT s.id, s.symbols
    INTO ln_subscriber_id, ltv_symbols
    FROM bina.subscribers s
    WHERE
          s.telegram_id = av_telegram_id
      AND s.acc_num = ln_acc_num;

    IF av_operation = '+' THEN

        SELECT ARRAY_AGG(y.symbol)
        INTO ltv_symbols_new
        FROM (SELECT DISTINCT x AS symbol
              FROM UNNEST(ltv_symbols) x
              UNION
              SELECT av_symbol) y;

    ELSIF av_operation = '-' THEN

        SELECT ARRAY_AGG(y.symbol)
        INTO ltv_symbols_new
        FROM (SELECT DISTINCT x AS symbol
              FROM UNNEST(ltv_symbols) x
              EXCEPT
              SELECT av_symbol) y;
    END IF;

    IF av_operation IN ('+', '-') THEN
        UPDATE bina.subscribers s
        SET
            symbols = ltv_symbols_new
        WHERE
            s.id = ln_subscriber_id;

        PERFORM bina.f_trade_log(an_subscriber_id => ln_subscriber_id,
                                 av_comment => 'set_config',
                                 aj_params => JSON_BUILD_OBJECT('telegram_id', av_telegram_id,
                                                                'acc_num', av_acc_num,
                                                                'symbol', av_symbol,
                                                                'operation', av_operation,
                                                                'error', av_error));
    END IF;

END;
$function$

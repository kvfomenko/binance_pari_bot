CREATE OR REPLACE FUNCTION bina.f_get_price_symbols(av_symbols OUT VARCHAR[])
    RETURNS VARCHAR[]
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE

BEGIN

    SELECT ARRAY_AGG(e.symbol)
    INTO av_symbols
    FROM bina.exchange_info e
    WHERE
          e.quote_asset = 'USDT'
      AND e.status = 'TRADING'
    --AND e.contract_type = 'PERPETUAL'
    ;

END;
$function$

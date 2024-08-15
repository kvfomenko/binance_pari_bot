CREATE OR REPLACE FUNCTION bina.f_save_exchange_info(aj_exchange_info JSON[])
    RETURNS VOID
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    cur               RECORD;
    ltv_order_types   VARCHAR(20)[];
    ltv_time_in_force VARCHAR(20)[];
BEGIN
    SET SESSION TIMEZONE TO UTC;

    FOR cur IN (SELECT
                    x ->> 'symbol' AS symbol,
                    --TO_TIMESTAMP((SUBSTR(x ->> 'time', 1, 9) || '0')::INTEGER)::TIMESTAMP AS price_date,
                    x ->> 'contractType' AS contract_type,
                    x ->> 'status' AS status,
                    (x ->> 'maintMarginPercent')::NUMERIC AS maint_margin_percent,
                    (x ->> 'requiredMarginPercent')::NUMERIC AS required_margin_percent,
                    x ->> 'baseAsset' AS base_asset,
                    x ->> 'quoteAsset' AS quote_asset,
                    (x ->> 'pricePrecision')::NUMERIC AS price_precision,
                    (x ->> 'quantityPrecision')::NUMERIC AS quantity_precision,
                    (x ->> 'baseAssetPrecision')::NUMERIC AS base_asset_precision,
                    (x ->> 'quotePrecision')::NUMERIC AS quote_precision,
                    x ->> 'underlyingType' AS underlying_type,
                    (x ->> 'triggerProtect')::NUMERIC AS trigger_protect,
                    (x ->> 'liquidationFee')::NUMERIC AS liquidation_fee,
                    (x ->> 'marketTakeBound')::NUMERIC AS market_take_bound,
                    (x ->> 'maxMoveOrderLimit')::NUMERIC AS max_move_order_limit,
                    (x ->> 'orderTypes')::JSON AS order_types,
                    (x ->> 'timeInForce')::JSON AS time_in_force,
                    (x ->> 'filters')::JSON AS filters
                FROM UNNEST(aj_exchange_info) x)
        LOOP

            SELECT ARRAY_AGG(a)
            INTO ltv_order_types
            FROM JSON_ARRAY_ELEMENTS_TEXT(cur.order_types) a;

            SELECT ARRAY_AGG(a)
            INTO ltv_time_in_force
            FROM JSON_ARRAY_ELEMENTS_TEXT(cur.time_in_force) a;

            INSERT INTO
                bina.exchange_info (symbol, update_date, contract_type, status, maint_margin_percent,
                                    required_margin_percent, base_asset, quote_asset, price_precision,
                                    quantity_precision, base_asset_precision, quote_precision, underlying_type,
                                    trigger_protect, liquidation_fee, market_take_bound, max_move_order_limit,
                                    order_types, time_in_force, filters)
            VALUES
                (cur.symbol, LOCALTIMESTAMP, cur.contract_type, cur.status, cur.maint_margin_percent,
                 cur.required_margin_percent, cur.base_asset, cur.quote_asset, cur.price_precision,
                 cur.quantity_precision, cur.base_asset_precision, cur.quote_precision, cur.underlying_type,
                 cur.trigger_protect, cur.liquidation_fee, cur.market_take_bound, cur.max_move_order_limit,
                 ltv_order_types, ltv_time_in_force, cur.filters)
            ON CONFLICT (symbol)
                DO UPDATE SET
                              update_date          = LOCALTIMESTAMP,
                              contract_type        = excluded.contract_type,
                              status               = excluded.status,
                              maint_margin_percent = excluded.maint_margin_percent,
                              base_asset           = excluded.base_asset,
                              quote_asset          = excluded.quote_asset,
                              price_precision      = excluded.price_precision,
                              quantity_precision   = excluded.quantity_precision,
                              base_asset_precision = excluded.base_asset_precision,
                              quote_precision      = excluded.quote_precision,
                              underlying_type      = excluded.underlying_type,
                              trigger_protect      = excluded.trigger_protect,
                              liquidation_fee      = excluded.liquidation_fee,
                              market_take_bound    = excluded.market_take_bound,
                              max_move_order_limit = excluded.max_move_order_limit,
                              order_types          = excluded.order_types,
                              time_in_force        = excluded.time_in_force,
                              filters              = excluded.filters;

        END LOOP;

END;
$function$

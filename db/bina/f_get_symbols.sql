CREATE OR REPLACE FUNCTION bina.f_get_symbols(av_telegram_id IN VARCHAR,
                                              av_acc_num IN     VARCHAR,
                                              av_config OUT     VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_acc_num                             SMALLINT := COALESCE(av_acc_num::SMALLINT, 1::SMALLINT);
    ln_subscriber_id                       INTEGER;
    cur                                    RECORD;
    ln                            CONSTANT VARCHAR  := CHR(10);
    g_max_blocked_symbols_in_list CONSTANT INTEGER  := 50;
BEGIN
    IF ln_acc_num > 1000 AND av_telegram_id = '313404677' THEN
        SELECT s.id
        INTO ln_subscriber_id
        FROM bina.subscribers s
        WHERE
            s.id = ln_acc_num - 1000;
    ELSE
        SELECT s.id
        INTO ln_subscriber_id
        FROM bina.subscribers s
        WHERE
              s.telegram_id = av_telegram_id
          AND s.acc_num = ln_acc_num;
    END IF;

    av_config := 'Symbols' || ln;
    FOR cur IN (SELECT
                    s.acc_num,
                    s.symbol_config ->> 'symbol_filter_mode' AS symbol_filter_mode,
                    CASE
                        WHEN s.symbol_config ->> 'symbol_filter_mode' = 'W' THEN ' (White list)'
                        WHEN s.symbol_config ->> 'symbol_filter_mode' = 'B' THEN ' (Black list)'
                        ELSE ' (Not used)'
                    END AS mode_description,
                    ARRAY_TO_STRING(s.symbols, ', ') AS symbols_list,
                    -----------------------------------------------------------
                    s.symbol_config ->> 'block_symbol_by_dev' AS block_symbol_by_dev,
                    s.symbol_config -> 'short' ->> 'block_symbol_short_by_dev' AS block_symbol_short_by_dev,
                    s.symbol_config -> 'long' ->> 'block_symbol_long_by_dev' AS block_symbol_long_by_dev,
                    s.symbol_config ->> 'block_symbol_dev_pc' AS block_symbol_dev_pc,
                    s.symbol_config ->> 'block_symbol_period_hr' AS block_symbol_period_hr,
                    /*CASE
                        WHEN s.symbol_config ->> 'block_symbol_by_dev' = 'Y'
                            THEN ' (Dynamic blocking applied)'
                        ELSE ' (Dynamic blocking not applied)'
                    END AS block_mode_description,*/
                    (SELECT
                         ARRAY_TO_STRING((ARRAY_AGG(' ' || x.symbol || ' ' || x.deviation || '% (' ||
                                                    RTRIM(x.min_price::TEXT, '0') ||
                                                    '-' || RTRIM(x.max_price::TEXT, '0') || ')'
                                                    ORDER BY x.deviation DESC))[1:g_max_blocked_symbols_in_list], ln)
                     FROM bina.v_sim_blocked x
                     WHERE
                         x.subscriber_id = s.id) AS symbols_block,
                    /*(SELECT
                         STRING_AGG(' ' || x.symbol || ' ' || x.deviation || '% (' || RTRIM(x.min_price::TEXT, '0') ||
                                    '-' || RTRIM(x.max_price::TEXT, '0') || ')', ln ORDER BY ABS(x.deviation) DESC)
                     FROM bina.v_sim_blocked x
                     WHERE
                         x.subscriber_id = s.id) AS symbols_block,*/
                    s.symbol_config ->> 'block_new_symbol_hr' AS block_new_symbol_hr,
                    COALESCE((SELECT
                                  STRING_AGG(' ' || x.symbol || ' (' || x.symbol_start_date || ')', ln)
                              FROM (SELECT n.symbol, n.symbol_start_date
                                    FROM bina.v_sim_blocked_new n
                                    WHERE
                                        n.subscriber_id = s.id
                                    ORDER BY n.symbol_start_date DESC
                                    LIMIT 30) x), '-') AS symbols_block_new,
                    s.symbol_config ->> 'block_symbol_avg_price_hr' AS block_symbol_avg_price_hr,
                    s.symbol_config ->> 'block_symbol_after_take_profit_seconds' AS block_symbol_after_take_profit_seconds,
                    s.symbol_config ->> 'block_symbol_after_stop_loss_minutes' AS block_symbol_after_stop_loss_minutes,
                    s.symbol_config ->> 'block_symbol_after_stop_loss_usdt' AS block_symbol_after_stop_loss_usdt,
                    s.symbol_config ->> 'pause_bot_after_stop_loss_count' AS pause_bot_after_stop_loss_count,
                    s.symbol_config ->> 'pause_bot_after_stop_loss_interval_sec' AS pause_bot_after_stop_loss_interval_sec
                FROM bina.subscribers s
                WHERE
                    s.id = ln_subscriber_id
                ORDER BY
                    s.acc_num)
        LOOP
            av_config := av_config || '--------------------------' || ln;
            av_config := av_config
                             || 'account: n' || cur.acc_num || ln
                             || '`/set n' || cur.acc_num || ' symbol_filter_mode` ' || cur.symbol_filter_mode ||
                         cur.mode_description || ln
                             || 'symbols list: ' || COALESCE(cur.symbols_list, '') || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_short_by_dev` ' ||
                         COALESCE(cur.block_symbol_short_by_dev, cur.block_symbol_by_dev) || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_long_by_dev` ' ||
                         COALESCE(cur.block_symbol_long_by_dev, cur.block_symbol_by_dev) || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_dev_pc` ' || cur.block_symbol_dev_pc || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_period_hr` ' || cur.block_symbol_period_hr ||
                         ln
                             || 'dynamically blocked symbols (top ' || g_max_blocked_symbols_in_list || '):' || ln ||
                         COALESCE(cur.symbols_block, '') || ln
                             || '`/set n' || cur.acc_num || ' block_new_symbol_hr` ' || cur.block_new_symbol_hr || ln
                             || 'blocked new symbols:' || ln || COALESCE(cur.symbols_block_new, '') || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_avg_price_hr` ' ||
                         COALESCE(cur.block_symbol_avg_price_hr, '0') || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_after_take_profit_seconds` ' ||
                         cur.block_symbol_after_take_profit_seconds || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_after_stop_loss_minutes` ' ||
                         cur.block_symbol_after_stop_loss_minutes || ln
                             || '`/set n' || cur.acc_num || ' block_symbol_after_stop_loss_usdt` ' ||
                         cur.block_symbol_after_stop_loss_usdt || ln
                             || '`/set n' || cur.acc_num || ' pause_bot_after_stop_loss_count` ' ||
                         cur.pause_bot_after_stop_loss_count || ln
                             || '`/set n' || cur.acc_num || ' pause_bot_after_stop_loss_interval_sec` ' ||
                         cur.pause_bot_after_stop_loss_interval_sec || ln;

        END LOOP;

    av_config := av_config || '--------------------------' || ln;
    av_config := av_config || 'available commands for symbols:' || ln;
    av_config := av_config || '`/symbol n1` BTCUSDT+ add symbol to the list' || ln;
    av_config := av_config || '`/symbol n1` BTCUSDT- remove symbol from the list';

END;
$function$

DROP FUNCTION IF EXISTS bina.f_get_profit_file(VARCHAR, VARCHAR, VARCHAR, OUT VARCHAR);

CREATE OR REPLACE FUNCTION bina.f_get_profit_file(av_telegram_id IN    VARCHAR,
                                                  av_acc_num IN        VARCHAR,
                                                  av_months IN         VARCHAR, -- last monthes
                                                  an_subscriber_id OUT INTEGER,
                                                  av_datetime OUT      VARCHAR, -- YYYYMMDD_HH24MI
                                                  av_file OUT          VARCHAR) -- csv
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_acc_num    SMALLINT   := COALESCE(av_acc_num::SMALLINT, 1::SMALLINT);
    cur           RECORD;
    ln CONSTANT   VARCHAR    := CHR(10);
    ld_from       TIMESTAMP;
    lj_csv_config JSON;
    lv_field_sep  VARCHAR(1);
    lv_num_sep    VARCHAR(1);
    lv_quote      VARCHAR(1) := '"';
BEGIN
    ld_from := DATE_TRUNC('month', LOCALTIMESTAMP) - (COALESCE(av_months, '0') || ' month')::INTERVAL;
    av_datetime := TO_CHAR(LOCALTIMESTAMP, 'YYYYMMDD_HH24MI');

    SELECT s.id, s.csv_config
    INTO an_subscriber_id, lj_csv_config
    FROM bina.subscribers s
    WHERE
          s.telegram_id = av_telegram_id
      AND s.acc_num = ln_acc_num;

    lv_field_sep := lj_csv_config ->> 'delimeter';
    lv_num_sep := lj_csv_config ->> 'number_delimeter';
    av_file :=
                'side|symbol|start_date|end_date|time|open_orders|close_type|position_usdt|profit|commission|profit_pc|ema|dev_pc|chart' ||
                ln;
    av_file := REPLACE(av_file, '|', lv_field_sep);

    FOR cur IN (SELECT
                    a.acc_num,
                    TO_CHAR(a.start_trading_date, 'YYYY-MM-DD HH24:MI:SS') AS start_date,
                    TO_CHAR(a.profit_date, 'YYYY-MM-DD HH24:MI:SS') AS profit_date,
                    TRUNC(DATE_PART('EPOCH', a.profit_date))
                        - TRUNC(DATE_PART('EPOCH', a.start_trading_date)) AS trade_time,
                    a.symbol,
                    a.trading_side,
                    a.filled_usdt,
                    a.profit_pc,
                    a.position_usdt,
                    a.commission_usdt,
                    CASE a.start_trading_params ->> 'ema_index'
                        WHEN '1' THEN 'EMA-1S'
                        WHEN '2' THEN 'EMA-3S'
                        WHEN '3' THEN 'EMA-5S'
                        WHEN '4' THEN 'EMA-10S'
                        WHEN '5' THEN 'EMA-60S'
                        WHEN '6' THEN 'EMA-5M'
                        ELSE ''
                    END AS ema,
                    COALESCE(a.start_trading_params ->> 'dev_pc', '') AS dev_pc,
                    CASE
                        WHEN a.sub_type = 'move_to_zero' THEN 'M'
                        --WHEN a.sub_type = 'close' then ''
                        ELSE ''
                    END ||
                    CASE
                        WHEN a.order_type = 'take_profit' AND a.sub_type = 'trailing' THEN 'TRL'
                        WHEN a.order_type = 'take_profit' AND a.sub_type = 'partial' THEN 'TPP'
                        WHEN a.order_type = 'take_profit' AND a.sub_type = 'second' THEN 'TPS'
                        WHEN a.order_type = 'take_profit' THEN 'TP'
                        WHEN a.order_type = 'stop_loss' THEN 'SL'
                        WHEN a.order_type = 'stop_loss_immediate' THEN 'SLI'
                        WHEN a.order_type = 'timeout' THEN 'OT'
                        WHEN a.order_type IN ('initial', 'avarage', 'third') THEN 'OPN'
                        ELSE 'MAN'
                    END AS close_type,
                    a.count_orders,
                    '/chart ' || a.symbol || ' ' ||
                    TO_CHAR((a.start_trading_date - INTERVAL '1 MINUTE')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI')
                        || ' ' || TO_CHAR((a.profit_date + INTERVAL '1 MINUTE')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI')
                        || ' n' || a.acc_num AS chart
                FROM (SELECT
                          s.acc_num,
                          p.symbol,
                          p.trading_side,
                          p.start_trading_date,
                          p.profit_date,
                          p.filled_usdt,
                          p.profit_pc,
                          p.order_type,
                          p.sub_type,
                          p.position_usdt,
                          p.commission_usdt,
                          (SELECT l.params
                           FROM bina.trade_log l
                           WHERE
                                 l.subscriber_id = p.subscriber_id
                             AND l.comment = 'start trading'
                             AND l.log_date = p.start_trading_date) AS start_trading_params,
                          (SELECT COUNT(1)
                           FROM bina.trade_log l2
                           WHERE
                                 l2.subscriber_id = p.subscriber_id
                             AND l2.comment = 'order trade update'
                             AND l2.params ->> 'symbol' = p.symbol
                             AND l2.params ->> 'orderStatus' = 'FILLED'
                             AND l2.params ->> 'order_type' IN ('initial', 'average', 'third')
                             AND l2.log_date BETWEEN p.start_trading_date AND p.profit_date) AS count_orders
                      FROM bina.subscribers s
                         , bina.v_all_profit p
                      WHERE
                            s.telegram_id = av_telegram_id
                        AND s.acc_num = ln_acc_num
                        AND s.id = p.subscriber_id
                        AND p.profit_date >= ld_from) a
                /*WHERE
                    a.filled_usdt != 0*/
                ORDER BY a.profit_date)
        LOOP
            av_file := av_file ||
                       cur.trading_side || lv_field_sep ||
                       cur.symbol || lv_field_sep ||
                       lv_quote || cur.start_date || lv_quote || lv_field_sep ||
                       lv_quote || cur.profit_date || lv_quote || lv_field_sep ||
                       /*lv_quote ||*/ cur.trade_time || /*lv_quote ||*/ lv_field_sep ||
                       cur.count_orders || lv_field_sep ||
                       lv_quote || cur.close_type || lv_quote || lv_field_sep ||
                       /*lv_quote || */REPLACE(cur.position_usdt::TEXT, '.', lv_num_sep) || /*lv_quote || */lv_field_sep ||
                       /*lv_quote ||*/ REPLACE(cur.filled_usdt::TEXT, '.', lv_num_sep) || /*lv_quote || */lv_field_sep ||
                       /*lv_quote ||*/ REPLACE(cur.commission_usdt::TEXT, '.', lv_num_sep) || /*lv_quote ||*/ lv_field_sep ||
                       /*lv_quote ||*/ REPLACE(cur.profit_pc::TEXT, '.', lv_num_sep) || /*lv_quote ||*/ lv_field_sep ||
                       cur.ema || lv_field_sep ||
                       /*lv_quote ||*/ REPLACE(cur.dev_pc::TEXT, '.', lv_num_sep) || /*lv_quote ||*/ lv_field_sep ||
                       lv_quote || cur.chart || lv_quote || ln;
        END LOOP;

END;
$function$

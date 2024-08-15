CREATE OR REPLACE FUNCTION bina.f_get_profit_history(av_telegram_id IN VARCHAR,
                                                     av_acc_num IN     VARCHAR,
                                                     av_profit OUT     VARCHAR)
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_acc_num                 SMALLINT := COALESCE(av_acc_num::SMALLINT, 1::SMALLINT);
    ln_subscriber_id           INTEGER;
    cur                        RECORD;
    ln CONSTANT                VARCHAR  := CHR(10);
    l_total_bets               NUMERIC  := 0;
    l_total_bets_positive      NUMERIC  := 0;
    l_total_bets_negative      NUMERIC  := 0;
    l_total_profit             NUMERIC  := 0;
    l_total_profit_positive    NUMERIC  := 0;
    l_total_profit_negative    NUMERIC  := 0;
    l_total_profit_pc          NUMERIC  := 0;
    l_total_profit_positive_pc NUMERIC  := 0;
    l_total_profit_negative_pc NUMERIC  := 0;
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

    av_profit := 'Profit' || ln;
    av_profit := av_profit || '--------------------------' || ln;

    FOR cur IN (SELECT
                    a.acc_num,
                    TO_CHAR(a.profit_date, 'MM/DD HH24:MI:SS') AS profit_date,
                    a.symbol,
                    a.trading_side,
                    a.profit,
                    a.profit_pc,
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
                        ELSE 'MAN'
                    END AS order_type,
                    /*case when a.sub_type = 'trailing' then 'TRL'
                         when a.order_type = 'take_profit' then 'TP'
                         when a.order_type = 'stop_loss' then 'SL'
                        else '' end as order_type,*/
                    a.count_orders,
                    '/chart ' || a.symbol
                        || ' ' ||
                    TO_CHAR((a.start_trading_dt - INTERVAL '1 minute')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI')
                        || ' ' || TO_CHAR((a.profit_date + INTERVAL '1 minute')::TIMESTAMP, 'YYYY-MM-DD_HH24:MI')
                        || ' n' || ln_acc_num AS chart,
                    ROW_NUMBER() OVER (ORDER BY a.profit_date DESC) AS pos
                FROM (SELECT
                          s.acc_num,
                          p.symbol,
                          p.trading_side,
                          p.profit_date,
                          p.filled_usdt AS profit,
                          p.profit_pc,
                          p.order_type,
                          p.sub_type,
                          p.start_trading_date AS start_trading_dt,
                          (SELECT COUNT(1)
                           FROM bina.trade_log l2
                           WHERE
                                 l2.subscriber_id = p.subscriber_id
                             AND l2.comment = 'order trade update'
                             AND l2.params ->> 'symbol' = p.symbol
                             AND l2.params ->> 'orderStatus' = 'FILLED'
                             AND l2.params ->> 'order_type' IN ('initial', 'average', 'third')
                             AND l2.log_date BETWEEN p.start_trading_date AND p.profit_date) AS count_orders
                      /*COALESCE((SELECT MAX(l2.log_date)
                                FROM bina.trade_log l2
                                WHERE
                                      l2.subscriber_id = p.subscriber_id
                                  AND l2.comment = 'start trading'
                                  AND l2.log_date BETWEEN p.profit_date - INTERVAL '7 day'
                                          AND p.profit_date
                                  AND l2.params ->> 'symbol' = p.symbol),
                               p.profit_date - INTERVAL '1 minute') AS start_trading_dt*/
                      FROM bina.subscribers s
                         , bina.v_all_profit p
                      WHERE
                            s.id = ln_subscriber_id
                        AND s.id = p.subscriber_id
                        AND p.profit_date >= DATE_TRUNC('month', LOCALTIMESTAMP)) a
                WHERE
                    a.profit != 0
                ORDER BY a.profit_date)
        LOOP
            IF cur.pos <= 20 THEN
                av_profit := av_profit || cur.trading_side || '(' || cur.count_orders || ') '
                                 || cur.order_type || ' '
                                 || CASE WHEN cur.profit > 0 THEN '+' ELSE '' END || cur.profit || '$ '
                                 || CASE WHEN cur.profit_pc > 0 THEN '+' ELSE '' END
                                 || cur.profit_pc || '% `' ||
                             cur.chart || '`' || ln;
            END IF;
            l_total_bets := l_total_bets + 1;
            l_total_profit := l_total_profit + cur.profit;
            l_total_profit_pc := l_total_profit_pc + cur.profit_pc;
            IF cur.profit >= 0 THEN
                l_total_bets_positive := l_total_bets_positive + 1;
                l_total_profit_positive := l_total_profit_positive + cur.profit;
                l_total_profit_positive_pc := l_total_profit_positive_pc + cur.profit_pc;
            ELSE
                l_total_bets_negative := l_total_bets_negative + 1;
                l_total_profit_negative := l_total_profit_negative + cur.profit;
                l_total_profit_negative_pc := l_total_profit_negative_pc + cur.profit_pc;
            END IF;
        END LOOP;

    av_profit := av_profit || '--------------------------' || ln;
    av_profit := av_profit || 'totals for current month: ' || ln
                     || 'count: ' || l_total_bets || '/+' || l_total_bets_positive || '/-' || l_total_bets_negative ||
                 ln
                     || 'sum: ' || CASE WHEN l_total_profit > 0 THEN '+' ELSE '' END || l_total_profit || '/+' ||
                 l_total_profit_positive || '/' || l_total_profit_negative || '$' || ln
                     || 'rate: ' || CASE WHEN l_total_profit_pc > 0 THEN '+' ELSE '' END || l_total_profit_pc || '/+' ||
                 l_total_profit_positive_pc || '/' || l_total_profit_negative_pc || '%' || ln ||
                 CASE
                     WHEN l_total_bets > 0 THEN
                                     'average: ' || CASE WHEN l_total_profit_pc > 0 THEN '+' ELSE '-' END
                                 || ROUND(l_total_profit_pc / l_total_bets, 1) || '%'
                     ELSE ''
                 END || '/' ||
                 CASE
                     WHEN l_total_bets_positive > 0 THEN
                                 '+' || ROUND(l_total_profit_positive_pc / l_total_bets_positive, 1) || '%'
                     ELSE '-'
                 END
                     || '/' ||
                 CASE
                     WHEN l_total_bets_negative > 0 THEN
                         ROUND(l_total_profit_negative_pc / l_total_bets_negative, 1) || '%'
                     ELSE '-'
                 END || ln;


END ;
$function$

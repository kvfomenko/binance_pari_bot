CREATE OR REPLACE FUNCTION bina.f_get_notifications(av_telegram_id IN VARCHAR,
                                                    av_acc_num IN     VARCHAR,
                                                    av_config OUT     VARCHAR)
    RETURNS VARCHAR
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ln_acc_num  SMALLINT := COALESCE(av_acc_num::SMALLINT, 1::SMALLINT);
    cur         RECORD;
    ln CONSTANT VARCHAR  := CHR(10);
BEGIN

    av_config := 'Notifications' || ln;
    FOR cur IN (SELECT
                    s.acc_num,
                    CASE
                        WHEN (s.notifications ->> 'new_event')::BOOLEAN = TRUE THEN 'Y'
                        ELSE 'N'
                    END AS new_event,
                    CASE
                        WHEN (s.notifications ->> 'created_order')::BOOLEAN = TRUE THEN 'Y'
                        ELSE 'N'
                    END AS created_order,
                    CASE
                        WHEN (s.notifications ->> 'failed_order')::BOOLEAN = TRUE THEN 'Y'
                        ELSE 'N'
                    END AS failed_order,
                    CASE
                        WHEN (s.notifications ->> 'blocked_symbol')::BOOLEAN = TRUE THEN 'Y'
                        ELSE 'N'
                    END AS blocked_symbol,
                    CASE
                        WHEN (s.notifications ->> 'canceled_order')::BOOLEAN = TRUE THEN 'Y'
                        ELSE 'N'
                    END AS canceled_order,
                    CASE
                        WHEN (s.notifications ->> 'opened_position')::BOOLEAN = TRUE THEN 'Y'
                        ELSE 'N'
                    END AS opened_position,
                    CASE
                        WHEN (s.notifications ->> 'realised_profit')::BOOLEAN = TRUE THEN 'Y'
                        ELSE 'N'
                    END AS realised_profit
                FROM bina.subscribers s
                WHERE
                      s.telegram_id = av_telegram_id
                  AND s.acc_num = ln_acc_num
                ORDER BY
                    s.acc_num)
        LOOP
            av_config := av_config || '--------------------------' || ln;
            av_config := av_config
                             || 'account: n' || cur.acc_num || ln
                             || '`/set n' || cur.acc_num || ' new_event` ' || cur.new_event || ln
                             || '`/set n' || cur.acc_num || ' created_order` ' || cur.created_order || ln
                             || '`/set n' || cur.acc_num || ' failed_order` ' || cur.failed_order || ln
                             || '`/set n' || cur.acc_num || ' blocked_symbol` ' || cur.blocked_symbol || ln
                             || '`/set n' || cur.acc_num || ' canceled_order` ' || cur.canceled_order || ln
                             || '`/set n' || cur.acc_num || ' opened_position` ' || cur.opened_position || ln
                             || '`/set n' || cur.acc_num || ' realised_profit` ' || cur.realised_profit || ln;
        END LOOP;

    av_config := av_config || '--------------------------' || ln;
    av_config := av_config || 'available commands for notifications:' || ln;
    av_config := av_config || '`/set n1 created_order Y` - turn on notification' || ln;
    av_config := av_config || '`/set n1 created_order N` - turn off notification';

END;
$function$

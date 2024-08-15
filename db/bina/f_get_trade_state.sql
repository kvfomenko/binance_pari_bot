CREATE OR REPLACE FUNCTION bina.f_get_trade_state()
    RETURNS SETOF RECORD
    SECURITY DEFINER
    VOLATILE
    LANGUAGE sql
AS
$function$

SELECT s.subscriber_id, s.state
FROM bina.trade_state s
WHERE
        (s.subscriber_id, s.update_date) IN (SELECT s.subscriber_id, MAX(s.update_date) AS update_date
                                             FROM bina.trade_state s
                                             GROUP BY s.subscriber_id)
ORDER BY s.subscriber_id;

$function$

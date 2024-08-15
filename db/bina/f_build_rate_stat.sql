DROP FUNCTION IF EXISTS bina.f_build_rate_stat();

CREATE OR REPLACE FUNCTION bina.f_build_rate_stat(av_backup_sql OUT VARCHAR[])
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    ld_prev_stat_dt     TIMESTAMP;
    ld_new_stat_from_dt TIMESTAMP;
    ld_new_stat_to_dt   TIMESTAMP;
    ld_now              TIMESTAMP;
BEGIN
    SET SESSION TIMEZONE TO UTC;
    ld_now := LOCALTIMESTAMP;

    SELECT MAX(s.price_date) + INTERVAL '1 hour'
    INTO ld_prev_stat_dt
    FROM bina.rates_stat s;

    IF ld_prev_stat_dt IS NULL THEN
        ld_prev_stat_dt := DATE_TRUNC('hour', LOCALTIMESTAMP - INTERVAL '24 hour');
    END IF;

    ld_new_stat_from_dt := ld_prev_stat_dt;
    ld_new_stat_to_dt := DATE_TRUNC('hour', LOCALTIMESTAMP);

    IF ld_new_stat_to_dt > ld_new_stat_from_dt THEN
        INSERT INTO
            bina.rates_stat (price_date, symbol, calc_date, min_price, avg_price, max_price)
        SELECT
            DATE_TRUNC('hour', r.price_date) AS price_date, r.symbol,
            ld_now,
            MIN(r.price)::REAL,
            AVG(r.price)::REAL,
            MAX(r.price)::REAL
        FROM bina.rates r
        WHERE
              r.price_date >= ld_prev_stat_dt
          AND r.price_date < ld_new_stat_to_dt
        GROUP BY
            DATE_TRUNC('hour', r.price_date), r.symbol
        ORDER BY
            DATE_TRUNC('hour', r.price_date), r.symbol;

        av_backup_sql := ARRAY(SELECT
           'INSERT INTO bina.rates_stat VALUES ("' ||
           DATE_TRUNC('hour', r.price_date) ||
           '","' ||
           r.symbol || '","' ||
           LOCALTIMESTAMP::TEXT || '",' ||
           MIN(r.price)::REAL || ',' ||
           AVG(r.price)::REAL ||
           ',' || MAX(r.price)::REAL ||
           ');' AS sql
                               FROM bina.rates r
                               WHERE
                                     r.price_date >= ld_prev_stat_dt
                                 AND r.price_date < ld_new_stat_to_dt
                               GROUP BY
                                   DATE_TRUNC('hour', r.price_date), r.symbol
                               ORDER BY
                                   DATE_TRUNC('hour', r.price_date), r.symbol);


        --PERFORM bina.f_trade_log(an_subscriber_id => 0, av_comment => 'build_rate_stat', aj_params => NULL);
    END IF;

END;
$function$

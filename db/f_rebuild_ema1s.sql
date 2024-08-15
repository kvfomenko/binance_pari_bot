CREATE OR REPLACE FUNCTION bina.f_rebuild_ema1s()
    RETURNS VOID
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    cur RECORD;
BEGIN
    SET SESSION TIMEZONE TO UTC;
/*
    create table bina.rates1s_tmp
(
    symbol     varchar,
    price_date timestamp,
    price      real,
    ema        real[]
);

alter table bina.rates1s_tmp
    owner to vou;

create index rates1s_tmp_symbol_price_date_index
    on bina.rates1s_tmp (symbol, price_date);

*/

    TRUNCATE TABLE bina.rates1s_tmp;

    FOR cur IN (SELECT x.symbol
                FROM bina.exchange_info x
                /*WHERE x.symbol = 'BAKEUSDT'*/)
        LOOP

            INSERT INTO
                bina.rates1s_tmp(symbol, price_date, price, ema)
            WITH
                RECURSIVE
                conf      AS (SELECT array[1,3,5,10,60] AS ema_size),
                rates_ema AS ((SELECT
                                   r.symbol, r.price_date, r.price, r.price AS ema1, r.price AS ema2, r.price AS ema3,
                                   r.price AS ema4, r.price AS ema5
                               FROM bina.rates1s r
                               WHERE
                                   r.symbol = cur.symbol
                                   --AND r.price_date = TO_TIMESTAMP('2023-10-08_13:00', 'YYYY-MM-DD HH24:MI')
                               ORDER BY
                                   r.price_date
                               LIMIT 1)
                              UNION ALL
                              SELECT
                                  r.symbol, r.price_date, r.price,
                                  (2::REAL / (s.ema_size[1] + 1)::REAL * l.price +
                                   (1 - 2::REAL / (s.ema_size[1] + 1)::REAL) * l.ema1)::REAL AS ema1,
                                  (2::REAL / (s.ema_size[2] + 1)::REAL * l.price +
                                   (1 - 2::REAL / (s.ema_size[2] + 1)::REAL) * l.ema2)::REAL AS ema2,
                                  (2::REAL / (s.ema_size[3] + 1)::REAL * l.price +
                                   (1 - 2::REAL / (s.ema_size[3] + 1)::REAL) * l.ema3)::REAL AS ema3,
                                  (2::REAL / (s.ema_size[4] + 1)::REAL * l.price +
                                   (1 - 2::REAL / (s.ema_size[4] + 1)::REAL) * l.ema4)::REAL AS ema4,
                                  (2::REAL / (s.ema_size[5] + 1)::REAL * l.price +
                                   (1 - 2::REAL / (s.ema_size[5] + 1)::REAL) * l.ema5)::REAL AS ema5
                              FROM conf s
                                 , rates_ema l
                                   JOIN bina.rates1s r ON (r.price_date = l.price_date + INTERVAL '1 seconds'
                                  AND r.symbol = l.symbol)
                    --where r.price_date <= TO_TIMESTAMP('2023-10-08_16:12', 'YYYY-MM-DD HH24:MI')
                )
            SELECT
                r.symbol,
                r.price_date,
                r.price,
                ARRAY [r.ema1, r.ema2, r.ema3, r.ema4, r.ema5] AS ema
            FROM rates_ema r;

        END LOOP;


END;
$function$

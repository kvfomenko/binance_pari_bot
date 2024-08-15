with par as (select 10 as minutes),
    t1 as (SELECT x.tm, count(1) as qty
from (
SELECT TO_CHAR(TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM l.log_date) / (par.minutes*60))) * par.minutes*60)::TIMESTAMP, 'HH24:MI') as tm
FROM par, bina.trade_log l
WHERE
      l.comment = 'order trade update'
  AND l.params ->> 'orderStatus' = 'NEW'
  AND l.params ->> 'order_type' = 'initial') x
group by x.tm),
    t0 as (SELECT lpad(h::varchar,2,'0') || ':' || lpad(m::varchar,2,'0') as tm
               from par, generate_series(0,23,1) h,
                    generate_series(0,59,par.minutes) m)
select t0.tm, coalesce(t1.qty,0) as qty,
       lpad('|', round((coalesce(t1.qty,0)::numeric) * 2, 0)::integer, '|') as graph1
    from t0
    left join t1 on (t1.tm = t0.tm)
order by t0;


insert into bina.rates_stat (price_date, symbol, calc_date, min_price, avg_price, max_price)
select s.price_date - interval '1 hour', s.symbol, s.price_date,
       (p2.min_price + s.min_price)/2,
       (p2.avg_price + s.avg_price)/2,
       (p2.max_price + s.max_price)/2
    from bina.rates_stat s
left join bina.rates_stat p on (p.symbol = s.symbol and p.price_date = s.price_date - interval '1 hour')
left join bina.rates_stat p2 on (p2.symbol = s.symbol
                                     and p2.price_date = s.price_date - interval '2 hour')
where s.price_date between '2023-10-21 20:00' and '2024-03-10 10:00'
and p.avg_price is null
and p2.avg_price is not null;

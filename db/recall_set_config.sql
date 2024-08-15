select l.*, 'select bina.f_set_config(''' || (l.params->>'telegram_id') || ''',
                                      ''' || (l.params->>'acc_num') || ''',
                                      ''' || (l.params->>'parameter') || ''',
                                      ''' || (l.params->>'value') || ''');' as sql
from bina.trade_log l
where l.subscriber_id != 1
and l.comment = 'set_config'
and l.log_date > '2023-11-08'
order by l.log_date asc;

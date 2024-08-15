create table bina.process
(
    last_process_time     timestamp,
    cleanup_threshold     timestamp,
    events_threshold      timestamp,
    last_deploy_date      timestamp,
    last_bot_restart_date timestamp
);


create table bina.subscribers
(
    id                         serial
        constraint subscribers_id_pk
            primary key,
    name                       varchar(50),
    phone_number               varchar(20),
    subscribe_time             timestamp,
    status                     varchar(1),
    trading_mode               varchar(1),
    binance_leverage           numeric,
    telegram_id                varchar(20),
    symbols                    character varying[],
    ema_index                  smallint,
    notifications              json,
    check_near_ema             smallint default 1,
    dev_short_pc               numeric,
    dev_long_pc                numeric,
    move_stop_loss_check_ema   numeric,
    csv_config                 json,
    test_mode                  varchar(1),
    last_deploy_date           timestamp,
    acc_num                    smallint,
    api_configured             varchar(1),
    api_validated              varchar(1),
    approved                   varchar(1),
    balance_request_last_date  timestamp,
    balance_usdt               numeric,
    api_validation_error       varchar(100),
    update_time                timestamp,
    last_trading_start_date    timestamp,
    trading_symbol             varchar(20),
    active_orders              smallint,
    active_orders_last_date    timestamp,
    active_positions           smallint,
    active_positions_last_date timestamp,
    config                     json,
    symbol_config              json,
    binance_acc                json,
    trading_side               varchar(10)
);

comment on column bina.subscribers.status is 'A - active, P - paused';

comment on column bina.subscribers.trading_mode is 'M - manual, A - full-auto, O - auto-open/manual-close';

comment on column bina.subscribers.test_mode is 'S - stable, B - beta';

comment on column bina.subscribers.api_configured is 'Y/N';

comment on column bina.subscribers.api_validated is 'Y - validated, N - not validated yet';

comment on column bina.subscribers.approved is 'Y - approved, N - not approved yet';

comment on column bina.subscribers.last_trading_start_date is 'null - no active trading';


create index subscribers_telegram_id_index
    on bina.subscribers (telegram_id);

create unique index subscribers_telegram_id_acc_num_uindex
    on bina.subscribers (telegram_id, acc_num);

create table bina.trade_log
(
    id            serial,
    log_date      timestamp,
    subscriber_id integer,
    comment       varchar(2000),
    params        json
);


create index trade_log_subscriber_id_index
    on bina.trade_log (subscriber_id, comment, log_date);

create table bina.exchange_info
(
    symbol                  varchar(20) not null,
    update_date             timestamp,
    contract_type           varchar(20),
    status                  varchar(20),
    maint_margin_percent    numeric,
    required_margin_percent numeric,
    base_asset              varchar(20),
    quote_asset             varchar(20),
    price_precision         smallint,
    quantity_precision      smallint,
    base_asset_precision    smallint,
    quote_precision         smallint,
    underlying_type         varchar(20),
    trigger_protect         numeric,
    liquidation_fee         numeric,
    market_take_bound       numeric,
    max_move_order_limit    numeric,
    order_types             varchar(20)[],
    time_in_force           varchar(10)[],
    filters                 json
);


create unique index exchange_info_symbol_uindex
    on bina.exchange_info (symbol);

create table bina.rates_stat
(
    price_date timestamp   not null,
    symbol     varchar(20) not null,
    calc_date  timestamp   not null,
    min_price  real,
    avg_price  real,
    max_price  real
);


create unique index rates_stat_price_date_symbol_uindex
    on bina.rates_stat (price_date, symbol);

create table bina.rates1s
(
    symbol             varchar(20),
    price_date         timestamp,
    price              real,
    ema                real[],
    event_id           smallint,
    status             smallint,
    core_client_id     smallint,
    load_time_ms       integer,
    binance_price_date timestamp,
    min_price          real,
    max_price          real
);


create unique index rates1s_symbol_price_date_uindex
    on bina.rates1s (symbol, price_date);

create index rates1s_event_id_index
    on bina.rates1s (event_id)
    where (event_id <> 0);

create index rates1s_price_date_index
    on bina.rates1s (price_date);

create table bina.rates1s_last
(
    symbol             varchar,
    price_date         timestamp,
    price              real,
    ema                real[],
    dev_pc             real[],
    binance_price_date timestamp,
    min_price          real,
    max_price          real,
    event_id           smallint
);


create unique index rates1s_last_symbol_uindex
    on bina.rates1s_last (symbol);

create table bina.binance_positions
(
    subscriber_id integer,
    symbol        varchar(20),
    update_date   timestamp,
    position      json
);


create unique index binance_positions_subscriber_id_symbol_uindex
    on bina.binance_positions (subscriber_id, symbol);

create table bina.trade_state
(
    subscriber_id integer,
    symbol        varchar(20),
    state         json,
    update_date   timestamp
);


create unique index trade_state_subscriber_id_symbol_uindex
    on bina.trade_state (subscriber_id, symbol);


create table bina.rates
(
    symbol     varchar(20),
    price_date timestamp,
    price      real,
    ema        real[],
    event_id   smallint,
    status     smallint,
    min_price  real,
    max_price  real
)
    partition by RANGE (price_date);


create table bina.rates_2023_10
    partition of bina.rates
        FOR VALUES FROM ('2023-10-01 00:00:00') TO ('2023-10-31 23:59:59')
    with (fillfactor = 90);


create table bina.rates_2023_11
    partition of bina.rates
        FOR VALUES FROM ('2023-11-01 00:00:00') TO ('2023-11-30 23:59:59')
    with (fillfactor = 90);


create table bina.rates_2023_12
    partition of bina.rates
        FOR VALUES FROM ('2023-12-01 00:00:00') TO ('2023-12-31 23:59:59')
    with (fillfactor = 90);


create table bina.rates_2024_01
    partition of bina.rates
        FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2024-01-31 23:59:59')
    with (fillfactor = 90);


create table bina.rates_2024_02
    partition of bina.rates
        FOR VALUES FROM ('2024-02-01 00:00:00') TO ('2024-02-29 23:59:59')
    with (fillfactor = 90);


create table bina.rates_2024_03
    partition of bina.rates
        FOR VALUES FROM ('2024-03-01 00:00:00') TO ('2024-03-31 23:59:59')
    with (fillfactor = 90);


create table bina.rates_2024_04
    partition of bina.rates
        FOR VALUES FROM ('2024-04-01 00:00:00') TO ('2024-04-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_05
    partition of bina.rates
        FOR VALUES FROM ('2024-05-01 00:00:00') TO ('2024-05-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_06
    partition of bina.rates
        FOR VALUES FROM ('2024-06-01 00:00:00') TO ('2024-06-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_07
    partition of bina.rates
        FOR VALUES FROM ('2024-07-01 00:00:00') TO ('2024-07-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_08
    partition of bina.rates
        FOR VALUES FROM ('2024-08-01 00:00:00') TO ('2024-08-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_09
    partition of bina.rates
        FOR VALUES FROM ('2024-09-01 00:00:00') TO ('2024-09-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_10
    partition of bina.rates
        FOR VALUES FROM ('2024-10-01 00:00:00') TO ('2024-10-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_11
    partition of bina.rates
        FOR VALUES FROM ('2024-11-01 00:00:00') TO ('2024-11-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2024_12
    partition of bina.rates
        FOR VALUES FROM ('2024-12-01 00:00:00') TO ('2024-12-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_01
    partition of bina.rates
        FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2025-01-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_02
    partition of bina.rates
        FOR VALUES FROM ('2025-02-01 00:00:00') TO ('2025-02-28 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_03
    partition of bina.rates
        FOR VALUES FROM ('2025-03-01 00:00:00') TO ('2025-03-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_04
    partition of bina.rates
        FOR VALUES FROM ('2025-04-01 00:00:00') TO ('2025-04-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_05
    partition of bina.rates
        FOR VALUES FROM ('2025-05-01 00:00:00') TO ('2025-05-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_06
    partition of bina.rates
        FOR VALUES FROM ('2025-06-01 00:00:00') TO ('2025-06-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_07
    partition of bina.rates
        FOR VALUES FROM ('2025-07-01 00:00:00') TO ('2025-07-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_08
    partition of bina.rates
        FOR VALUES FROM ('2025-08-01 00:00:00') TO ('2025-08-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_09
    partition of bina.rates
        FOR VALUES FROM ('2025-09-01 00:00:00') TO ('2025-09-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_10
    partition of bina.rates
        FOR VALUES FROM ('2025-10-01 00:00:00') TO ('2025-10-31 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_11
    partition of bina.rates
        FOR VALUES FROM ('2025-11-01 00:00:00') TO ('2025-11-30 23:59:59')
    with (fillfactor = 99);


create table bina.rates_2025_12
    partition of bina.rates
        FOR VALUES FROM ('2025-12-01 00:00:00') TO ('2025-12-31 23:59:59')
    with (fillfactor = 99);


create unique index rates_symbol_price_date_uindex
    on bina.rates (symbol, price_date);

create index rates_event_id_index
    on bina.rates (event_id)
    where (event_id <> 0);

create index rates_price_date_index
    on bina.rates (price_date);



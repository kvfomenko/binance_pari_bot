CREATE OR REPLACE FUNCTION bina.f_get_chart1s(av_telegram_id IN VARCHAR,
                                              av_acc_num IN     VARCHAR,
                                              av_symbol IN      VARCHAR,
                                              av_from IN        VARCHAR, -- YYYY-MM-DD HH24:MI
                                              av_to IN          VARCHAR, -- YYYY-MM-DD HH24:MI
                                              aj_chart OUT      JSON,
                                              av_template OUT   VARCHAR)
    SECURITY DEFINER
    VOLATILE
    LANGUAGE plpgsql
AS
$function$
DECLARE
    g_rows_max CONSTANT INTEGER   := 2000;

    ld_from             TIMESTAMP := TO_TIMESTAMP(REPLACE(av_from, '\', ''), 'YYYY-MM-DD HH24:MI');
    ld_to               TIMESTAMP := TO_TIMESTAMP(REPLACE(av_to, '\', ''), 'YYYY-MM-DD HH24:MI');
    ln_acc_num          SMALLINT;
    lv_scale            VARCHAR;
    lv_trunc            VARCHAR;
    ln_rows             INTEGER;
    ln_price            REAL;
    ln_round_digits     INTEGER   := 4;
    ln_subscriber_id    INTEGER;
    ln_ema_index        SMALLINT;
    lv_labels           VARCHAR   := '';
    lv_prices           VARCHAR   := '';
    lv_min_prices       VARCHAR   := '';
    lv_max_prices       VARCHAR   := '';
    lv_ema1             VARCHAR   := '';
    lv_ema2             VARCHAR   := '';
    lv_ema3             VARCHAR   := '';
    lv_ema4             VARCHAR   := '';
    lv_ema5             VARCHAR   := '';
    lv_ema6             VARCHAR   := '';
    lv_trading_events   VARCHAR;
BEGIN
    BEGIN
        ln_acc_num := av_acc_num::SMALLINT;
    EXCEPTION
        WHEN OTHERS THEN
            ln_acc_num := 1;
    END;

    SELECT s.id, s.ema_index
    INTO ln_subscriber_id, ln_ema_index
    FROM bina.subscribers s
    WHERE
          s.telegram_id = av_telegram_id
      AND s.acc_num = ln_acc_num;

    PERFORM bina.f_trade_log(ln_subscriber_id, 'chart', JSON_BUILD_OBJECT('acc_num', av_acc_num,
                                                                          'symbol', av_symbol,
                                                                          'from', av_from,
                                                                          'to', av_to));

    IF av_to::DATE - av_from::DATE >= 3 THEN
        av_template := '<html><body>Period very big. Please use not more then 2 days</body></html>';
        aj_chart := JSON_BUILD_OBJECT('subscriber_id', ln_subscriber_id,
                                      'symbol', av_symbol,
                                      'from', TO_CHAR(ld_from, 'YYYYMMDD_HH24MI'),
                                      'to', TO_CHAR(ld_to, 'YYYYMMDD_HH24MI'),
                                      'ema_index', ln_ema_index,
                                      'labels', '',
                                      'prices', '',
                                      'ema1', '',
                                      'ema2', '',
                                      'ema3', '',
                                      'ema4', '',
                                      'ema5', '',
                                      'ema6', '');
        RETURN;
    END IF;

    av_template := '<html><head>
<style>
body,div {width: 1600px; height: 800px;}
</style>
</head><body>
<div><canvas id="myChart"></canvas></div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@3.0.0/dist/chart.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.0.0"></script>
<script>
    const data = {
    labels: [{{labels}}],
    datasets: [
{type: "line", label: "{{symbol}} price", hidden: true, borderWidth: 1, backgroundColor: "rgb(0, 0, 0)", borderColor: "rgb(0, 0, 0)", tension: 0.2, data: [{{prices}}]},
{type: "line", label: "{{symbol}} min-price", borderWidth: 1, backgroundColor: "rgb(128, 128, 128)", borderColor: "rgb(128, 128, 128)", tension: 0.2, data: [{{min_prices}}]},
{type: "line", label: "{{symbol}} max-price", borderWidth: 1, backgroundColor: "rgb(128, 128, 128)", borderColor: "rgb(128, 128, 128)", tension: 0.2, data: [{{max_prices}}]},
{type: "line", label: "PREV-1s", hidden: {{hidden_ema1}}, borderWidth: 1, backgroundColor: "rgb(128, 128, 128)", borderColor: "rgb(128, 128, 128)", tension: 0.2, data: [{{ema1}}] },
{type: "line", label: "EMA-3", hidden: {{hidden_ema2}}, borderWidth: 1, backgroundColor: "rgb(255, 0, 0)", borderColor: "rgb(255, 0, 0)", tension: 0.2, data: [{{ema2}}] },
{type: "line", label: "EMA-5", hidden: {{hidden_ema3}}, borderWidth: 1, backgroundColor: "rgb(0, 255, 0)", borderColor: "rgb(0, 255, 0)", tension: 0.2, data: [{{ema3}}] },
{type: "line", label: "EMA-10", hidden: {{hidden_ema4}}, borderWidth: 1, backgroundColor: "rgb(0, 0, 255)", borderColor: "rgb(0, 0, 255)", tension: 0.2, data: [{{ema4}}] },
{type: "line", label: "EMA-60", hidden: {{hidden_ema5}}, borderWidth: 1, backgroundColor: "rgb(255, 0, 255)", borderColor: "rgb(255, 0, 255)", tension: 0.2, data: [{{ema5}}] },
{type: "line", label: "EMA-5m", hidden: {{hidden_ema6}}, borderWidth: 1, backgroundColor: "rgb(128, 128, 255)", borderColor: "rgb(128, 128, 255)", tension: 0.2, data: [{{ema6}}] },
{type: "bubble", label: "Trades", backgroundColor: "rgb(255, 128, 128)",  borderColor: "rgb(255, 128, 128)", data: [{{trading_events}}] }
  ]};
    const datasets = data.datasets.length;
    function trimToDecimalPlaces(number, precision) {
        const array = number.toString().split(''.'');
        array.push(array.pop().substring(0, precision));
        const trimmedstr = array.join(''.'');
        return parseFloat(trimmedstr);
    }
    const footer = (tooltipItems) => {
        let result, price, ema = [], dev = [];
        tooltipItems.forEach(function(tooltipItem) {
            if (tooltipItem.datasetIndex == 0) {
                price = data.datasets[0].data[tooltipItem.dataIndex];
                result = "Deviation:";
                for (let i=0+3;i<=datasets-2;i++) {
                    ema[i] = data.datasets[i].data[tooltipItem.dataIndex];
                    dev[i] = trimToDecimalPlaces(price / ema[i] * 100 - 100, 2);
                    result += "\n" + data.datasets[i].label + ": " + dev[i] + "%";
                }
            } else if (tooltipItem.datasetIndex == datasets-1) {
                result = data.datasets[tooltipItem.datasetIndex].data[tooltipItem.dataIndex].c;
            }
        });
        return result;
    };
  const ctx = document.getElementById("myChart");
  new Chart(ctx, {
    plugins: [ChartDataLabels],
    type: "line",
    data: data,
    options: {
    plugins: {legend: {position: "top", labels: {usePointStyle: false, pointStyleWidth: 1}},
		tooltip: {callbacks: {footer: footer}},
        datalabels: {color: "white", font: {weight: "bold"}, offset: 2, padding: 0,
            formatter: function(value) {
                if (value.v) {
                    return value.v
                } else {
                    return " "
                }
            }
        }
    },
    animation: {duration:0},
    elements: {point: {radius:2}},
    scales: {y: {beginAtZero: false}}
    }
  });
</script>
</body></html>';

    SELECT COUNT(1)
    INTO ln_rows
    FROM bina.rates1s r
    WHERE
          r.symbol = av_symbol
      AND r.price_date BETWEEN ld_from AND ld_to;

    IF ln_rows < g_rows_max THEN
        lv_scale := '1s';
    ELSIF ln_rows < g_rows_max * 10 THEN
        lv_scale := '10s';
    ELSIF ln_rows < g_rows_max * 10 * 6 THEN
        lv_scale := '1m';
    ELSIF ln_rows < g_rows_max * 10 * 6 * 10 THEN
        lv_scale := '10m';
    ELSIF ln_rows < g_rows_max * 10 * 6 * 10 * 6 THEN
        lv_scale := '1h';
    ELSE
        lv_scale := '1d';
    END IF;

    IF lv_scale = '1s' THEN
        lv_trunc := 'second';
    ELSIF lv_scale = '10s' THEN
        lv_trunc := 'second';
    ELSIF lv_scale = '1m' THEN
        lv_trunc := 'minute';
    ELSIF lv_scale = '10m' THEN
        lv_trunc := 'minute';
    ELSIF lv_scale = '1h' THEN
        lv_trunc := 'hour';
    ELSIF lv_scale = '1d' THEN
        lv_trunc := 'day';
    END IF;

    SELECT price
    INTO ln_price
    FROM bina.rates1s_last
    WHERE
        symbol = av_symbol;

    ln_round_digits := 6 - LOG(ln_price);

    RAISE INFO 'rows %, scale %, trunc %, price %, round_digits %', ln_rows, lv_scale, lv_trunc, ln_price, ln_round_digits;

    IF lv_scale = '10s' THEN
        SELECT
            STRING_AGG('''' || TO_CHAR(x.price_date, 'MM/DD HH24:MI:SS') || '''', ',' ORDER BY x.price_date),
            STRING_AGG(x.price::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.min_price::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.max_price::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[1]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[2]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[3]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[4]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[5]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[6]::VARCHAR, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices, lv_min_prices, lv_max_prices, lv_ema1, lv_ema2, lv_ema3, lv_ema4, lv_ema5, lv_ema6
        FROM (SELECT
                  r.symbol,
                  TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM r.price_date) / 10)) * 10)::TIMESTAMP AS price_date,
                  ROUND(AVG(r.price)::NUMERIC, ln_round_digits) AS price,
                  ROUND(AVG(r.min_price)::NUMERIC, ln_round_digits) AS min_price,
                  ROUND(AVG(r.max_price)::NUMERIC, ln_round_digits) AS max_price,
                  ARRAY [ROUND(AVG(r.ema[1])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[2])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[3])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[4])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[5])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[6])::NUMERIC, ln_round_digits)] AS ema
              FROM bina.rates1s r
              WHERE
                    r.symbol = av_symbol
                AND r.ema[1] IS NOT NULL
                AND r.price_date BETWEEN ld_from AND ld_to
              GROUP BY
                  r.symbol, TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM r.price_date) / 10)) * 10)::TIMESTAMP) x
        GROUP BY
            x.symbol;

    ELSIF lv_scale = '10m' THEN
        SELECT
            STRING_AGG('''' || TO_CHAR(x.price_date, 'MM/DD HH24:MI:SS') || '''', ','
                       ORDER BY x.price_date),
            STRING_AGG(x.price::VARCHAR, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices
        FROM (SELECT
                  r.symbol,
                  TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM r.price_date) / 600)) * 600)::TIMESTAMP AS price_date,
                  ROUND(AVG(r.price)::NUMERIC, ln_round_digits) AS price
              FROM bina.rates1s r
              WHERE
                    r.symbol = av_symbol
                AND r.ema[1] IS NOT NULL
                AND r.price_date BETWEEN ld_from AND ld_to
              GROUP BY
                  r.symbol, TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM r.price_date) / 600)) * 600)::TIMESTAMP) x
        GROUP BY
            x.symbol;

    ELSIF lv_scale IN ('1s', '1m') THEN
        SELECT
            STRING_AGG('''' || TO_CHAR(x.price_date, 'MM/DD HH24:MI:SS') || '''', ','
                       ORDER BY x.price_date),
            STRING_AGG(x.price::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.min_price::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.max_price::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[1]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[2]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[3]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[4]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[5]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[6]::VARCHAR, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices, lv_min_prices, lv_max_prices, lv_ema1, lv_ema2, lv_ema3, lv_ema4, lv_ema5, lv_ema6
        FROM (SELECT
                  r.symbol,
                  DATE_TRUNC(lv_trunc, r.price_date) AS price_date,
                  ROUND(AVG(r.price)::NUMERIC, ln_round_digits) AS price,
                  ROUND(AVG(r.min_price)::NUMERIC, ln_round_digits) AS min_price,
                  ROUND(AVG(r.max_price)::NUMERIC, ln_round_digits) AS max_price,
                  ARRAY [ROUND(AVG(r.ema[1])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[2])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[3])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[4])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[5])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[6])::NUMERIC, ln_round_digits)] AS ema
              FROM bina.rates1s r
              WHERE
                    r.symbol = av_symbol
                AND r.ema[1] IS NOT NULL
                AND r.price_date BETWEEN ld_from AND ld_to
              GROUP BY
                  r.symbol, DATE_TRUNC(lv_trunc, r.price_date)) x
        GROUP BY
            x.symbol;

    ELSE
        SELECT
            STRING_AGG('''' || TO_CHAR(x.price_date, 'MM/DD HH24:MI:SS') || '''', ',' ORDER BY x.price_date),
            STRING_AGG(x.price::VARCHAR, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices
        FROM (SELECT
                  r.symbol,
                  DATE_TRUNC(lv_trunc, r.price_date) AS price_date,
                  ROUND(AVG(r.price)::NUMERIC, ln_round_digits) AS price
              FROM bina.rates1s r
              WHERE
                    r.symbol = av_symbol
                AND r.ema[1] IS NOT NULL
                AND r.price_date BETWEEN ld_from AND ld_to
              GROUP BY
                  r.symbol, DATE_TRUNC(lv_trunc, r.price_date)) x
        GROUP BY
            x.symbol;

    END IF;

    SELECT
        STRING_AGG(' {x:"' || TO_CHAR(
                CASE
                    WHEN lv_scale = '10s' THEN TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM x.dt) / 10)) * 10)::TIMESTAMP
                    WHEN lv_scale = '10m' THEN TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM x.dt) / 600)) * 600)::TIMESTAMP
                    ELSE DATE_TRUNC(lv_trunc, x.dt)
                END
            , 'MM/DD HH24:MI:SS') || '", y:' || x.price || ', r: 15, v: "' || x.label ||
                   '", c:"' || x.comment || '"}', ',')
    INTO lv_trading_events
    FROM (SELECT
              TO_TIMESTAMP((l.params ->> 'orderTradeTime')::BIGINT / 1000)::TIMESTAMP AS dt,
              CASE
                  WHEN l.params ->> 'orderStatus' = 'FILLED' THEN l.params ->> 'averagePrice'
                  ELSE l.params ->> 'originalPrice'
              END AS price,
              CASE
                  WHEN l.params ->> 'orderStatus' = 'FILLED' THEN
                      l.params ->> 'orderSide'
                  ELSE
                      LOWER(l.params ->> 'orderSide')
              END AS label,
              COALESCE(l.params ->> 'order_type', '') || '\n'
                  || CASE
                         WHEN (l.params ->> 'realisedProfit')::NUMERIC != 0 THEN
                                 ' realisedProfit: ' ||
                                 COALESCE('$' || ROUND((l.params ->> 'realisedProfit')::NUMERIC, 2)::TEXT, '-')
                         ELSE ''
                     END AS comment
          FROM bina.trade_log l
          WHERE
                l.subscriber_id = ln_subscriber_id
            AND l.log_date BETWEEN ld_from AND ld_to
            AND l.comment IN ('order trade update', 'order outside bot-trading update')
            AND l.params ->> 'orderStatus' IN ('FILLED', 'NEW')
            AND l.params ->> 'symbol' = av_symbol
            AND CASE
                    WHEN l.params ->> 'orderStatus' = 'FILLED' THEN l.params ->> 'averagePrice'
                    ELSE l.params ->> 'originalPrice'
                END::NUMERIC != 0) x;

    aj_chart := JSON_BUILD_OBJECT('subscriber_id', ln_subscriber_id,
                                  'symbol', av_symbol,
                                  'scale', lv_scale,
                                  'from', TO_CHAR(ld_from, 'YYYYMMDD_HH24MI'),
                                  'to', TO_CHAR(ld_to, 'YYYYMMDD_HH24MI'),
                                  'ema_index', ln_ema_index,
                                  'labels', lv_labels,
                                  'prices', lv_prices,
                                  'min_prices', lv_min_prices,
                                  'max_prices', lv_max_prices,
                                  'ema1', lv_ema1,
                                  'ema2', lv_ema2,
                                  'ema3', lv_ema3,
                                  'ema4', lv_ema4,
                                  'ema5', lv_ema5,
                                  'ema6', COALESCE(lv_ema6, ''));

    av_template := REPLACE(av_template, '{{symbol}}', av_symbol);
    av_template := REPLACE(av_template, '{{labels}}', lv_labels);
    av_template := REPLACE(av_template, '{{prices}}', lv_prices);
    av_template := REPLACE(av_template, '{{min_prices}}', COALESCE(lv_min_prices, ''));
    av_template := REPLACE(av_template, '{{max_prices}}', COALESCE(lv_max_prices, ''));
    av_template := REPLACE(av_template, '{{ema1}}', COALESCE(lv_ema1, ''));
    av_template := REPLACE(av_template, '{{ema2}}', COALESCE(lv_ema2, ''));
    av_template := REPLACE(av_template, '{{ema3}}', COALESCE(lv_ema3, ''));
    av_template := REPLACE(av_template, '{{ema4}}', COALESCE(lv_ema4, ''));
    av_template := REPLACE(av_template, '{{ema5}}', COALESCE(lv_ema5, ''));
    av_template := REPLACE(av_template, '{{ema6}}', COALESCE(lv_ema6, ''));
    av_template := REPLACE(av_template, '{{trading_events}}', COALESCE(lv_trading_events, ''));

    IF ln_ema_index = 1 THEN
        av_template := REPLACE(av_template, '{{hidden_ema1}}', 'false');
    ELSE
        av_template := REPLACE(av_template, '{{hidden_ema1}}', 'true');
    END IF;
    IF ln_ema_index = 2 THEN
        av_template := REPLACE(av_template, '{{hidden_ema2}}', 'false');
    ELSE
        av_template := REPLACE(av_template, '{{hidden_ema2}}', 'true');
    END IF;
    IF ln_ema_index = 3 THEN
        av_template := REPLACE(av_template, '{{hidden_ema3}}', 'false');
    ELSE
        av_template := REPLACE(av_template, '{{hidden_ema3}}', 'true');
    END IF;
    IF ln_ema_index = 4 THEN
        av_template := REPLACE(av_template, '{{hidden_ema4}}', 'false');
    ELSE
        av_template := REPLACE(av_template, '{{hidden_ema4}}', 'true');
    END IF;
    IF ln_ema_index = 5 THEN
        av_template := REPLACE(av_template, '{{hidden_ema5}}', 'false');
    ELSE
        av_template := REPLACE(av_template, '{{hidden_ema5}}', 'true');
    END IF;
    IF ln_ema_index = 6 THEN
        av_template := REPLACE(av_template, '{{hidden_ema6}}', 'false');
    ELSE
        av_template := REPLACE(av_template, '{{hidden_ema6}}', 'true');
    END IF;

END ;
$function$

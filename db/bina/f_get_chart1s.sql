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
    g_rows_max CONSTANT    INTEGER     := 800;

    gv_color_up            VARCHAR(30) := '"rgb(0,200,0)"';
    gv_color_down          VARCHAR(30) := '"rgb(200,0,0)"';
    ld_from                TIMESTAMP   := TO_TIMESTAMP(REPLACE(av_from, '\', ''), 'YYYY-MM-DD HH24:MI');
    ld_to                  TIMESTAMP   := TO_TIMESTAMP(REPLACE(av_to, '\', ''), 'YYYY-MM-DD HH24:MI');
    ln_acc_num             INTEGER;
    lv_scale               VARCHAR;
    lv_trunc               VARCHAR;
    ln_rows                INTEGER;
    ln_price               REAL;
    ln_round_digits        INTEGER     := 4;
    ln_subscriber_id       INTEGER;
    ln_ema_index           SMALLINT;
    ln_trailing_pc         NUMERIC;
    lv_order_side          VARCHAR;
    ld_initial_order_date  TIMESTAMP;
    ld_trailing_end_dt     TIMESTAMP;
    ld_trailing_start_date TIMESTAMP[];
    ld_trailing_end_date   TIMESTAMP[];
    lv_labels              VARCHAR     := '';
    lv_prices              VARCHAR     := '';
    lv_colors              VARCHAR     := '"rgb(150,150,150)"';
    --lv_ema1             VARCHAR   := '';
    lv_ema2                VARCHAR     := '';
    lv_ema3                VARCHAR     := '';
    lv_ema4                VARCHAR     := '';
    lv_ema5                VARCHAR     := '';
    lv_ema6                VARCHAR     := '';
    lv_trailing            VARCHAR     := '';
    lv_trading_events      VARCHAR;
    lv_price_data_type     VARCHAR     := 'line';
    lj_config              JSON;
BEGIN
    BEGIN
        ln_acc_num := av_acc_num::INTEGER;
    EXCEPTION
        WHEN OTHERS THEN
            ln_acc_num := 1;
    END;
    ld_to := ld_to + INTERVAL '59 second';

    IF ln_acc_num > 1000 AND av_telegram_id = '313404677' THEN
        SELECT s.id, s.ema_index, s.config
        INTO ln_subscriber_id, ln_ema_index, lj_config
        FROM bina.subscribers s
        WHERE
            s.id = ln_acc_num - 1000;
    ELSE
        SELECT s.id, s.ema_index, s.config
        INTO ln_subscriber_id, ln_ema_index, lj_config
        FROM bina.subscribers s
        WHERE
              s.telegram_id = av_telegram_id
          AND s.acc_num = ln_acc_num;
    END IF;

    PERFORM bina.f_trade_log(ln_subscriber_id, 'chart', JSON_BUILD_OBJECT('acc_num', av_acc_num,
                                                                          'symbol', av_symbol,
                                                                          'from', av_from,
                                                                          'to', av_to));

    IF av_to::DATE - av_from::DATE >= 4 THEN
        av_template := '<html><body>Period very big. Please use not more then 2 days</body></html>';
        aj_chart := JSON_BUILD_OBJECT('subscriber_id', ln_subscriber_id,
                                      'symbol', av_symbol,
                                      'from', TO_CHAR(ld_from, 'YYYYMMDD_HH24MI'),
                                      'to', TO_CHAR(ld_to, 'YYYYMMDD_HH24MI'),
                                      'ema_index', ln_ema_index,
                                      'trailing_pc', 0,
                                      'labels', '',
                                      'prices', '',
                                      'trading_events', '',
                                      'colors', '',
                                      'trailing', '',
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
<div>
<div id="selected_dev" width="100px" height="30px" style="position:absolute;pointer-events:none;"></div>
<canvas id="overlay" width="600" height="400" style="position:absolute;pointer-events:none;"></canvas>
<canvas id="myChart"></canvas>
</div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@3.0.0/dist/chart.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.0.0"></script>
<script>
    const data = {
    labels: [{{labels}}],
    datasets: [
{type: "{{price_data_type}}", label: "{{symbol}} price", hidden: false, borderWidth: 0, tension: 0.1, barPercentage: 1,
    backgroundColor: [{{colors}}],
    data: [{{prices}}]},
{type: "line", label: "Trailing {{trailing_pc}}%", hidden: false, borderWidth: 1, backgroundColor: "rgb(0, 200, 200)", borderColor: "rgb(0, 200, 200)", tension: 0.1, data: [{{trailing}}] },
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
        let datasets_ema_from = 2; ema_count = 5;
        tooltipItems.forEach(function(tooltipItem) {
            if (tooltipItem.datasetIndex == 0) {
                price = data.datasets[0].data[tooltipItem.dataIndex];
                result = "Deviation:";
                for (let i=datasets_ema_from; i<datasets_ema_from+ema_count; i++) {
                    ema[i] = data.datasets[i].data[tooltipItem.dataIndex];
                    dev[i] = trimToDecimalPlaces(price[0] / ema[i] * 100 - 100, 3) + '' .. '' + trimToDecimalPlaces(price[1] / ema[i] * 100 - 100, 3);
                    result += "\n" + data.datasets[i].label + ": " + dev[i] + "%";
                }
            } else if (tooltipItem.datasetIndex == datasets-1) {
                result = data.datasets[tooltipItem.datasetIndex].data[tooltipItem.dataIndex].c;
            }
        });
        return result;
    };
  const ctx = document.getElementById("myChart");
  var chart = new Chart(ctx, {
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
    scales: {y: {beginAtZero: false, grace: "0%"}}
    }
  });

  //selection
  let x1, x2, dev, prev_x;
  const selected_dev = document.getElementById("selected_dev");
  selected_dev.style.fontWeight = "900";
  selected_dev.style.fontFamily = "Tahoma";

  var overlay = document.getElementById("overlay");
    var startIndex = null, endIndex = null;
    overlay.width = ctx.width;
    overlay.height = ctx.height;
    var selectionContext = overlay.getContext("2d");
    var selectionRect = {w: 0, startX: 0, startY: 0};
    var drag = false;
    ctx.addEventListener("pointerdown", evt => {
      const points = chart.getElementsAtEventForMode(evt, "index", { intersect: false});
      if (points[0]) {
        startIndex = points[0].index;
        endIndex = null;
      }
      console.log("startIndex", startIndex);
      const rect = ctx.getBoundingClientRect();
      selectionRect.startX = evt.clientX - rect.left;
      selectionRect.startY = chart.chartArea.top;
      drag = true;
    });
    ctx.addEventListener("pointermove", evt => {
      const rect = ctx.getBoundingClientRect();
      if (drag) {
        const rect = ctx.getBoundingClientRect();
        selectionRect.w = (evt.clientX - rect.left) - selectionRect.startX;
        selectionContext.globalAlpha = 0.1;
        selectionContext.clearRect(0, 0, ctx.width, ctx.height);
        selectionContext.fillRect(selectionRect.startX, selectionRect.startY, selectionRect.w, chart.chartArea.bottom - chart.chartArea.top);
        refreshSelectedDeviation(evt);
        prev_x = evt.clientX;
      } else {
        //console.log("prev_x", prev_x, evt.clientX);
        if (Math.abs(evt.clientX - prev_x) > 50) {
            // clear selected after mouse move
            selectionContext.clearRect(0, 0, ctx.width, ctx.height);
        }
      }
    });
    ctx.addEventListener("pointerup", evt => {
      drag = false;
      refreshSelectedDeviation(evt);
    });

    function refreshSelectedDeviation(evt) {
	  const points = chart.getElementsAtEventForMode(evt, "index", {intersect: false});
      if (points[0]) {
        endIndex = points[0].index;
      }
      console.log("startIndex", startIndex, "endIndex", endIndex);
      if (endIndex > startIndex) {
        if (data.datasets[0].data[startIndex][0]) {
          x1 = (data.datasets[0].data[startIndex][0] + data.datasets[0].data[startIndex][1]) /2;
          x2 = (data.datasets[0].data[endIndex][0] + data.datasets[0].data[endIndex][1]) /2;
        } else {
          x1 = data.datasets[0].data[startIndex];
          x2 = data.datasets[0].data[endIndex];
        }
      } else if (endIndex < startIndex) {
        if (data.datasets[0].data[startIndex][0]) {
          x1 = (data.datasets[0].data[endIndex][0] + data.datasets[0].data[endIndex][1]) /2;
          x2 = (data.datasets[0].data[startIndex][0] + data.datasets[0].data[startIndex][1]) /2;
        } else {
          x1 = data.datasets[0].data[endIndex];
          x2 = data.datasets[0].data[startIndex];
        }
      }
      if (x1 && x2) {
        dev = trimToDecimalPlaces((x2 - x1) / x1 * 100, 1);
        console.log("x1", x1, "x2", x2, "dev", dev);
		if (dev > 0) {
		  selected_dev.style.color = "green";
		  selected_dev.innerHTML = "&nbsp; +" + dev + "% &nbsp;&nbsp;&nbsp;&nbsp;" + trimToDecimalPlaces(x1,6) + " - "  + trimToDecimalPlaces(x2,6);
		} else if (dev < 0) {
		  selected_dev.style.color = "red";
		  selected_dev.innerHTML = "&nbsp; " + dev + "% &nbsp;&nbsp;&nbsp;&nbsp;" + trimToDecimalPlaces(x1,6) + " - "  + trimToDecimalPlaces(x2,6);
		}
      }
	}

    function trimToDecimalPlaces(number, precision) {
        if (number) {
            let array = number.toString().split(".");
            if (!array[1]) {
                array.push("0");
            }
            array.push(array.pop().substring(0, precision));
            const trimmedstr = array.join(".");
            console.log("trimmedstr", trimmedstr);
            //return parseFloat(trimmedstr);
            return trimmedstr;
        } else {
            console.log("trimToDecimalPlaces error: no value");
            return "0";
        }
    }
</script>
</body></html>';

    SELECT COUNT(1)
    INTO ln_rows
    FROM bina.rates r
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
        lv_price_data_type := 'bar';
    ELSIF lv_scale = '10s' THEN
        lv_trunc := 'second';
        lv_price_data_type := 'bar';
    ELSIF lv_scale = '1m' THEN
        lv_trunc := 'minute';
        lv_price_data_type := 'bar';
    ELSIF lv_scale = '10m' THEN
        lv_trunc := 'minute';
        lv_price_data_type := 'bar';
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
            --STRING_AGG(x.price::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG('[' || x.min_price || ', ' || x.max_price || ']', ',' ORDER BY x.price_date),
            STRING_AGG(CASE
                           WHEN x.close_price >= (x.max_price - x.min_price) / 2 + x.min_price THEN gv_color_up
                           ELSE gv_color_down
                       END, ',' ORDER BY x.price_date),
            --STRING_AGG(x.ema[1]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[2]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[3]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[4]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[5]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[6]::VARCHAR, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices, lv_colors, lv_ema2, lv_ema3, lv_ema4, lv_ema5, lv_ema6
        FROM (SELECT
                  r.symbol,
                  TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM r.price_date) / 10)) * 10)::TIMESTAMP AS price_date,
                  --ROUND(AVG(r.price)::NUMERIC, ln_round_digits) AS price,
                  ROUND((ARRAY_AGG(r.price ORDER BY r.price_date DESC))[1]::NUMERIC, ln_round_digits) AS close_price,
                  ROUND(MIN(r.min_price)::NUMERIC, ln_round_digits) AS min_price,
                  ROUND(MAX(r.max_price)::NUMERIC, ln_round_digits) AS max_price,
                  ARRAY [ROUND(AVG(r.ema[1])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[2])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[3])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[4])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[5])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[6])::NUMERIC, ln_round_digits)] AS ema
              FROM bina.rates r
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
            --STRING_AGG(x.price::VARCHAR, ',' ORDER BY x.price_date)
        STRING_AGG('[' || x.min_price || ', ' || x.max_price || ']', ',' ORDER BY x.price_date),
            STRING_AGG(CASE
                           WHEN x.close_price >= (x.max_price - x.min_price) / 2 + x.min_price THEN gv_color_up
                           ELSE gv_color_down
                       END, ',' ORDER BY x.price_date),
            --STRING_AGG(x.ema[1]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[2]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[3]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[4]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[5]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[6]::VARCHAR, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices, lv_colors, lv_ema2, lv_ema3, lv_ema4, lv_ema5, lv_ema6
        FROM (SELECT
                  r.symbol,
                  TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM r.price_date) / 600)) * 600)::TIMESTAMP AS price_date,
                  ROUND((ARRAY_AGG(r.price ORDER BY r.price_date DESC))[1]::NUMERIC, ln_round_digits) AS close_price,
                  ROUND(MIN(r.min_price)::NUMERIC, ln_round_digits) AS min_price,
                  ROUND(MAX(r.max_price)::NUMERIC, ln_round_digits) AS max_price,
                  ARRAY [ROUND(AVG(r.ema[1])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[2])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[3])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[4])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[5])::NUMERIC, ln_round_digits),
                      ROUND(AVG(r.ema[6])::NUMERIC, ln_round_digits)] AS ema
              FROM bina.rates r
              WHERE
                    r.symbol = av_symbol
                AND r.ema[1] IS NOT NULL
                AND r.price_date BETWEEN ld_from AND ld_to
              GROUP BY
                  r.symbol, TO_TIMESTAMP(FLOOR((EXTRACT('epoch' FROM r.price_date) / 600)) * 600)::TIMESTAMP) x
        GROUP BY
            x.symbol;

    ELSIF lv_scale IN ('1s') THEN
        SELECT
            STRING_AGG('''' || TO_CHAR(x.price_date, 'MM/DD HH24:MI:SS') || '''', ','
                       ORDER BY x.price_date),
            /*STRING_AGG('{"x":"' || TO_CHAR(x.price_date, 'MM/DD HH24:MI:SS')
                           || '","o":' || x.open_price || ', "c":' || x.close_price ||
                       ', "l":' || x.min_price || ', "h":' || x.max_price || '}',
                       ',' ORDER BY x.price_date),*/
            STRING_AGG('[' || x.min_price || ', ' || x.max_price || ']', ',' ORDER BY x.price_date),
            STRING_AGG(CASE
                           WHEN x.close_price >= (x.max_price - x.min_price) / 2 + x.min_price THEN gv_color_up
                           ELSE gv_color_down
                       END, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[2]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[3]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[4]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[5]::VARCHAR, ',' ORDER BY x.price_date),
            STRING_AGG(x.ema[6]::VARCHAR, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices, lv_colors, lv_ema2, lv_ema3, lv_ema4, lv_ema5, lv_ema6
        FROM (SELECT
                  r.symbol,
                  r.price_date,
                  /*ROUND(CASE
                            WHEN
                                LAG(r.price) OVER (ORDER BY r.price_date) BETWEEN r.min_price AND r.max_price
                                THEN LAG(r.price) OVER (ORDER BY r.price_date)
                            WHEN LAG(r.price) OVER (ORDER BY r.price_date) < r.min_price THEN r.min_price
                            WHEN LAG(r.price) OVER (ORDER BY r.price_date) > r.max_price THEN r.max_price
                            ELSE r.price
                        END::NUMERIC, ln_round_digits) AS open_price,*/
                  ROUND(r.price::NUMERIC, ln_round_digits) AS close_price,
                  ROUND(r.min_price::NUMERIC, ln_round_digits) AS min_price,
                  ROUND(r.max_price::NUMERIC, ln_round_digits) AS max_price,
                  ARRAY [ROUND(r.ema[1]::NUMERIC, ln_round_digits),
                      ROUND(r.ema[2]::NUMERIC, ln_round_digits),
                      ROUND(r.ema[3]::NUMERIC, ln_round_digits),
                      ROUND(r.ema[4]::NUMERIC, ln_round_digits),
                      ROUND(r.ema[5]::NUMERIC, ln_round_digits),
                      ROUND(r.ema[6]::NUMERIC, ln_round_digits)] AS ema
              FROM bina.rates r
              WHERE
                    r.symbol = av_symbol
                AND r.ema[1] IS NOT NULL
                AND r.price_date BETWEEN ld_from AND ld_to) x
        GROUP BY
            x.symbol;

    ELSE
        SELECT
            STRING_AGG('''' || TO_CHAR(x.price_date, 'MM/DD HH24:MI:SS') || '''', ',' ORDER BY x.price_date),
            --STRING_AGG(x.price::VARCHAR, ',' ORDER BY x.price_date)
            STRING_AGG('[' || x.min_price || ', ' || x.max_price || ']', ',' ORDER BY x.price_date),
            STRING_AGG(CASE
                           WHEN x.close_price >= (x.max_price - x.min_price) / 2 + x.min_price THEN gv_color_up
                           ELSE gv_color_down
                       END, ',' ORDER BY x.price_date)
        INTO lv_labels, lv_prices, lv_colors
        FROM (SELECT
                  r.symbol,
                  DATE_TRUNC(lv_trunc, r.price_date) AS price_date,
                  --ROUND(AVG(r.price)::NUMERIC, ln_round_digits) AS price
                  ROUND((ARRAY_AGG(r.price ORDER BY r.price_date DESC))[1]::NUMERIC, ln_round_digits) AS close_price,
                  ROUND(MIN(r.min_price)::NUMERIC, ln_round_digits) AS min_price,
                  ROUND(MAX(r.max_price)::NUMERIC, ln_round_digits) AS max_price
              FROM bina.rates r
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
            , 'MM/DD HH24:MI:SS') || '", y:' || x.price || ', r: ' || x.bubble_size || ', v: "' || x.label ||
                   '", c:"' || x.comment || '"}', ',')
    INTO lv_trading_events
    FROM (SELECT
              TO_TIMESTAMP((a.params ->> 'orderTradeTime')::BIGINT / 1000)::TIMESTAMP AS dt,
              CASE
                  WHEN a.ordertype IN ('STOP_MARKET', 'TRAILING_STOP_MARKET')
                      THEN a.params ->> 'stopPrice'
                  WHEN a.order_status = 'FILLED' THEN a.params ->> 'averagePrice'
                  ELSE a.params ->> 'originalPrice'
              END AS price,
              CASE
                  WHEN a.order_status = 'FILLED' THEN
                      a.side
                  ELSE
                      LOWER(SUBSTR(a.side, 1, 1))
              END AS label,
              CASE
                  WHEN a.order_status = 'FILLED' THEN
                      15
                  ELSE
                      8
              END AS bubble_size,
              COALESCE(a.order_type ||
                       CASE
                           WHEN a.order_type IN ('take_profit', 'stop_loss') AND
                                POSITION('.' IN SUBSTRING(a.coid, POSITION('.' IN a.coid) + 1)) > 0
                               THEN
                                   '-' || SUBSTRING(SUBSTRING(a.coid, POSITION('.' IN a.coid) + 1),
                                                    1,
                                                    POSITION('.' IN SUBSTRING(a.coid, POSITION('.' IN a.coid) + 1)) - 1
                                          )
                           WHEN a.order_type IN ('take_profit', 'stop_loss') AND a.position_usdt != 0 AND
                                ABS((a.filled_usdt + COALESCE((a.partially_filled ->> 'profit')::NUMERIC, 0)) /
                                    a.position_usdt *
                                    100 /*profit_pc*/) < 1 THEN
                               '-move_to_zero'
                           ELSE ''
                       END /*sub_type*/

                  , '') || '\n'
                  || CASE
                         WHEN a.filled_usdt != 0 THEN
                                 ' realisedProfit: ' ||
                                 COALESCE('$' ||
                                          ROUND(a.filled_usdt + COALESCE((a.partially_filled ->> 'profit')::NUMERIC, 0),
                                                2)::TEXT,
                                          '-')
                         ELSE ''
                     END AS comment
          FROM (SELECT --l.*,
                       l.params,
                       l.params ->> 'orderStatus' AS order_status,
                       l.params ->> 'orderSide' AS side,
                       l.params ->> 'orderType' AS ordertype,
                       l.params ->> 'order_type' AS order_type,
                       l.params ->> 'clientOrderId' AS coid,
                       (l.params ->> 'averagePrice')::NUMERIC AS position_usdt,
                       (l.params ->> 'realisedProfit')::NUMERIC AS filled_usdt,
                       --(l.params ->> 'commissionAmount')::NUMERIC AS commission_usdt,
                       (SELECT
                            JSON_BUILD_OBJECT(
                                    'profit', SUM((l2.params ->> 'realisedProfit')::NUMERIC),
                                    'commission', SUM((l2.params ->> 'commissionAmount')::NUMERIC))
                        FROM bina.trade_log l2
                        WHERE
                              l.params ->> 'orderStatus' = 'FILLED' -- only for close orders
                          AND l2.subscriber_id = l.subscriber_id
                          AND l2.log_date BETWEEN l.log_date - INTERVAL '1 day' AND l.log_date
                          AND l2.comment IN ('order trade update', 'order outside bot-trading update')
                          AND l2.params ->> 'orderStatus' = 'PARTIALLY_FILLED'
                          AND l2.params ->> 'symbol' = l.params ->> 'symbol'
                          AND l2.params ->> 'orderId' = l.params ->> 'orderId') AS partially_filled
                FROM bina.trade_log l
                WHERE
                      l.subscriber_id = ln_subscriber_id
                  AND l.log_date BETWEEN ld_from AND ld_to
                  AND l.comment IN ('order trade update', 'order outside bot-trading update')
                  AND l.params ->> 'orderStatus' IN ('FILLED', 'NEW')
                  AND l.params ->> 'symbol' = av_symbol
                  AND CASE
                          WHEN l.params ->> 'orderStatus' = 'FILLED' THEN l.params ->> 'averagePrice'
                          WHEN l.params ->> 'orderType' IN ('STOP_MARKET', 'TRAILING_STOP_MARKET')
                              THEN l.params ->> 'stopPrice'
                          ELSE l.params ->> 'originalPrice'
                      END::NUMERIC != 0) a) x;


    SELECT l.params ->> 'orderSide', DATE_TRUNC('second', l.log_date)
    INTO lv_order_side, ld_initial_order_date
    FROM bina.trade_log l
    WHERE
          l.subscriber_id = ln_subscriber_id
      AND l.log_date BETWEEN ld_from AND ld_to
      AND l.comment = 'order trade update'
      AND l.params ->> 'orderStatus' = 'FILLED'
      AND l.params ->> 'symbol' = av_symbol
      AND l.params ->> 'order_type' = 'initial'
    ORDER BY l.log_date
    LIMIT 1;

    ld_trailing_start_date := ARRAY(
            SELECT
                DATE_TRUNC('second',
                           TO_TIMESTAMP((l.params ->> 'orderTradeTime')::BIGINT / 1000)::TIMESTAMP /*l.log_date*/)
            FROM bina.trade_log l
            WHERE
                  l.subscriber_id = ln_subscriber_id
              AND l.log_date BETWEEN ld_from AND ld_to
              AND l.comment = 'order trade update'
              AND l.params ->> 'orderStatus' = 'NEW'
              AND l.params ->> 'orderType' = 'TRAILING_STOP_MARKET'
              AND l.params ->> 'symbol' = av_symbol
              AND l.params ->> 'order_type' = 'take_profit'
              AND l.params ->> 'clientOrderId' LIKE '%trailing%'
            ORDER BY l.log_date);

    IF ARRAY_LENGTH(ld_trailing_start_date, 1) > 0 THEN
        FOR i IN 1..ARRAY_LENGTH(ld_trailing_start_date, 1) LOOP
            SELECT
                DATE_TRUNC('second',
                           TO_TIMESTAMP((l.params ->> 'orderTradeTime')::BIGINT / 1000)::TIMESTAMP /*l.log_date*/)
            INTO ld_trailing_end_dt
            FROM bina.trade_log l
            WHERE
                  l.subscriber_id = ln_subscriber_id
              AND l.log_date BETWEEN COALESCE(ld_trailing_start_date[i], ld_from) AND ld_to
              AND l.comment = 'order trade update'
              AND l.params ->> 'orderType' != 'LIMIT'
              AND l.params ->> 'orderStatus' = 'FILLED'
              AND l.params ->> 'symbol' = av_symbol
            --AND l.params ->> 'order_type' = 'take_profit'
            --AND l.params ->> 'clientOrderId' LIKE '%trailing%'
            ORDER BY l.log_date
            LIMIT 1;

            raise INFO 'ld_trailing_end_dt % %..%', i, ld_trailing_start_date[i], ld_trailing_end_dt;
            ld_trailing_end_date[i] := ld_trailing_end_dt - INTERVAL '1 second';
        END LOOP;
    END IF;

    /*IF ld_trailing_start_date[1] IS NULL THEN
        ld_trailing_start_date[1] := ld_initial_order_date;
    END IF;
    IF ld_trailing_end_date[1] IS NULL THEN
        ld_trailing_end_date[1] := ld_to;
    END IF;
    IF ld_trailing_end_date[2] IS NULL THEN
        ld_trailing_end_date[2] := ld_to;
    END IF;*/

    /*PERFORM bina.f_trade_log(ln_subscriber_id, 'chart-debug', JSON_BUILD_OBJECT('acc_num', av_acc_num,
                                                                      'symbol', av_symbol,
                                                                      'trailing_start_date', ld_trailing_start_date,
                                                                      'trailing_end_date', ld_trailing_end_date));*/

    IF ld_trailing_start_date[1] IS NOT NULL THEN
        IF lv_order_side = 'SELL' THEN
            ln_trailing_pc := (lj_config -> 'short' ->> 'trailing_stop_short_callback_pc')::NUMERIC;

            IF ln_trailing_pc > 0 THEN
                WITH
                    RECURSIVE
                    xx AS (SELECT
                               r.price_date, r.min_price, r.max_price,
                               (1 + ln_trailing_pc / 100) * r.max_price AS stop_price,
                               (1 + ln_trailing_pc / 100) * r.max_price AS trailing_stop_price
                           FROM bina.rates r
                           WHERE
                                 r.symbol = av_symbol
                             AND r.price_date = ld_from --ld_trailing_start_date
                           UNION
                           SELECT
                               r.price_date, r.min_price, r.max_price,
                               (1 + ln_trailing_pc / 100) * r.min_price AS stop_price,
                               CASE
                                   WHEN r.price_date = ANY (ld_trailing_start_date) THEN
                                       (1 + ln_trailing_pc / 100) * r.max_price
                                   ELSE
                                       LEAST(xx.trailing_stop_price, (1 + ln_trailing_pc / 100) * r.min_price)
                               END AS trailing_stop_price
                           FROM xx
                              , bina.rates r
                           WHERE
                                 r.symbol = av_symbol
                             AND r.price_date = xx.price_date + INTERVAL '1 second'
                             AND (r.price_date <= ld_to --ld_trailing_end_date
                               OR r.max_price >= LEAST(xx.trailing_stop_price, (1 + ln_trailing_pc / 100) * r.min_price)
                                     ))
                SELECT
                    STRING_AGG(aa.trailing_stop_price, ',' ORDER BY aa.price_date)
                INTO lv_trailing
                FROM (SELECT
                          xx.price_date,
                          CASE
                              WHEN xx.price_date BETWEEN ld_trailing_start_date[1] AND ld_trailing_end_date[1] THEN
                                  ROUND(xx.trailing_stop_price::NUMERIC, ln_round_digits)::VARCHAR
                              WHEN xx.price_date BETWEEN ld_trailing_start_date[2] AND ld_trailing_end_date[2] THEN
                                  ROUND(xx.trailing_stop_price::NUMERIC, ln_round_digits)::VARCHAR
                              ELSE 'null'
                          END
                              AS trailing_stop_price
                      FROM xx) aa;
            END IF;
        ELSE
            ln_trailing_pc := (lj_config -> 'long' ->> 'trailing_stop_long_callback_pc')::NUMERIC;

            IF ln_trailing_pc > 0 THEN
                WITH
                    RECURSIVE
                    xx AS (SELECT
                               r.price_date, r.min_price, r.max_price,
                               (1 - ln_trailing_pc / 100) * r.min_price AS stop_price,
                               (1 - ln_trailing_pc / 100) * r.min_price AS trailing_stop_price
                           FROM bina.rates r
                           WHERE
                                 r.symbol = av_symbol
                             AND r.price_date = ld_from --ld_trailing_start_date
                           UNION
                           SELECT
                               r.price_date, r.min_price, r.max_price,
                               (1 - ln_trailing_pc / 100) * r.max_price AS stop_price,
                               CASE
                                   WHEN r.price_date = ANY (ld_trailing_start_date) THEN
                                       (1 - ln_trailing_pc / 100) * r.max_price
                                   ELSE
                                       GREATEST(xx.trailing_stop_price, (1 - ln_trailing_pc / 100) * r.max_price)
                               END AS trailing_stop_price
                           FROM xx
                              , bina.rates r
                           WHERE
                                 r.symbol = av_symbol
                             AND r.price_date = xx.price_date + INTERVAL '1 second'
                             AND (r.price_date <= ld_to --ld_trailing_end_date
                               OR r.min_price <=
                                  GREATEST(xx.trailing_stop_price, (1 - ln_trailing_pc / 100) * r.max_price)
                                     ))
                SELECT
                    STRING_AGG(aa.trailing_stop_price, ',' ORDER BY aa.price_date)
                INTO lv_trailing
                FROM (SELECT
                          xx.price_date,
                          CASE
                              WHEN xx.price_date BETWEEN ld_trailing_start_date[1] AND ld_trailing_end_date[1] THEN
                                  ROUND(xx.trailing_stop_price::NUMERIC, ln_round_digits)::VARCHAR
                              WHEN xx.price_date BETWEEN ld_trailing_start_date[2] AND ld_trailing_end_date[2] THEN
                                  ROUND(xx.trailing_stop_price::NUMERIC, ln_round_digits)::VARCHAR
                              ELSE 'null'
                          END
                              AS trailing_stop_price
                      FROM xx) aa;
            END IF;
        END IF;
    END IF;

    RAISE INFO 'trailing:%, %, init:%, trail-start:%, trail-end:%', lv_order_side, ln_trailing_pc, ld_initial_order_date, ld_trailing_start_date, ld_trailing_end_date;

    aj_chart := JSON_BUILD_OBJECT('subscriber_id', ln_subscriber_id,
                                  'symbol', av_symbol,
                                  'scale', lv_scale,
                                  'from', TO_CHAR(ld_from, 'YYYYMMDD_HH24MI'),
                                  'to', TO_CHAR(ld_to, 'YYYYMMDD_HH24MI'),
                                  'ema_index', ln_ema_index,
                                  'trailing_pc', ln_trailing_pc,
                                  'trailing', lv_trailing,
                                  'trailing_start_date', ld_trailing_start_date,
                                  'ld_trailing_end_date', ld_trailing_end_date,
                                  'labels', lv_labels,
                                  'price_data_type', lv_price_data_type,
                                  'prices', lv_prices,
                                  'trading_events', lv_trading_events,
                                  'colors', lv_colors,
                                  'ema2', lv_ema2,
                                  'ema3', lv_ema3,
                                  'ema4', lv_ema4,
                                  'ema5', lv_ema5,
                                  'ema6', COALESCE(lv_ema6, ''));

    av_template := REPLACE(av_template, '{{symbol}}', av_symbol);
    av_template := REPLACE(av_template, '{{labels}}', COALESCE(lv_labels, ''));
    av_template := REPLACE(av_template, '{{prices}}', COALESCE(lv_prices, ''));
    av_template := REPLACE(av_template, '{{colors}}', COALESCE(lv_colors, ''));
    av_template := REPLACE(av_template, '{{price_data_type}}', lv_price_data_type);
    av_template := REPLACE(av_template, '{{trailing_pc}}', COALESCE(ln_trailing_pc::VARCHAR, ''));
    av_template := REPLACE(av_template, '{{trailing}}', COALESCE(lv_trailing, ''));
    --av_template := REPLACE(av_template, '{{ema1}}', COALESCE(lv_ema1, ''));
    av_template := REPLACE(av_template, '{{ema2}}', COALESCE(lv_ema2, ''));
    av_template := REPLACE(av_template, '{{ema3}}', COALESCE(lv_ema3, ''));
    av_template := REPLACE(av_template, '{{ema4}}', COALESCE(lv_ema4, ''));
    av_template := REPLACE(av_template, '{{ema5}}', COALESCE(lv_ema5, ''));
    av_template := REPLACE(av_template, '{{ema6}}', COALESCE(lv_ema6, ''));
    av_template := REPLACE(av_template, '{{trading_events}}', COALESCE(lv_trading_events, ''));

    av_template := REPLACE(av_template, '{{hidden_ema2}}', 'true');
    av_template := REPLACE(av_template, '{{hidden_ema3}}', 'true');
    av_template := REPLACE(av_template, '{{hidden_ema4}}', 'true');
    av_template := REPLACE(av_template, '{{hidden_ema5}}', 'true');
    av_template := REPLACE(av_template, '{{hidden_ema6}}', 'true');
    /*IF ln_ema_index = 2 THEN
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
    END IF;*/

END ;
$function$

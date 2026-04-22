-- CORE PAPER TABLE: mechanism × regime, all headline metrics
WITH runs AS (
  SELECT
    run_id,
    CASE WHEN run_id LIKE 'crash_%' THEN 'crash' ELSE 'control' END AS regime
  FROM tick_data
  WHERE run_id LIKE 'crash_btc_oct10_%' OR run_id LIKE 'control_btc_sep20_%'
  GROUP BY 1, 2
)
SELECT
  r.regime,
  t.mechanism,
  COUNT(*) AS fills,
  ROUND(AVG(t.slippage)::numeric, 4)        AS mean_slip,
  ROUND(STDDEV(t.slippage)::numeric, 4)     AS sd_slip,
  ROUND(percentile_cont(0.5)  WITHIN GROUP (ORDER BY t.slippage)::numeric, 4) AS p50_slip,
  ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY t.slippage)::numeric, 4) AS p95_slip,
  ROUND(percentile_cont(0.99) WITHIN GROUP (ORDER BY t.slippage)::numeric, 4) AS p99_slip,
  ROUND(MAX(t.slippage)::numeric, 2)        AS max_slip,
  ROUND(AVG(t.latency_delta)::numeric / 1000, 2) AS mean_lat_ms,
  ROUND(AVG(ABS(t.spread))::numeric, 4)     AS mean_abs_spread
FROM tick_data t
JOIN runs r USING (run_id)
WHERE t.mechanism IN ('CLOB','FBA10MS','FBA100MS','FBA200MS','FBA500MS',
                       'THROTTLED1MS','THROTTLED10MS','THROTTLED100MS')
GROUP BY r.regime, t.mechanism
ORDER BY r.regime DESC, t.mechanism;

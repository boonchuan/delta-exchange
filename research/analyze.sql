-- ══════════════════════════════════════════════════════════════
--  ΔEXCHANGE Research Analysis Queries
-- ══════════════════════════════════════════════════════════════

-- 1. Overall fill statistics by instrument
SELECT '=== FILL STATISTICS ===' as section;
SELECT
    symbol,
    COUNT(*) as total_fills,
    ROUND(AVG(fill_price)::numeric, 4) as avg_price,
    ROUND(STDDEV(fill_price)::numeric, 6) as price_stddev,
    ROUND(MIN(fill_price)::numeric, 4) as min_price,
    ROUND(MAX(fill_price)::numeric, 4) as max_price,
    ROUND(AVG(slippage)::numeric, 8) as avg_slippage
FROM tick_data
WHERE mechanism = 'DETERMINISTIC'
GROUP BY symbol
ORDER BY total_fills DESC;

-- 2. Time-series fill rate (per 5 minutes)
SELECT '=== FILL RATE (5-MIN BUCKETS) ===' as section;
SELECT
    date_trunc('minute', time) - (EXTRACT(minute FROM time)::int % 5) * interval '1 minute' as bucket,
    COUNT(*) as fills,
    COUNT(DISTINCT symbol) as active_instruments,
    ROUND(AVG(fill_price)::numeric, 4) as avg_price
FROM tick_data
GROUP BY bucket
ORDER BY bucket DESC
LIMIT 20;

-- 3. Price distribution analysis (for thesis)
SELECT '=== PRICE DISTRIBUTION ===' as section;
SELECT
    symbol,
    ROUND(AVG(fill_price)::numeric, 4) as mean,
    ROUND(STDDEV(fill_price)::numeric, 6) as stddev,
    ROUND((STDDEV(fill_price) / AVG(fill_price) * 100)::numeric, 4) as cv_pct,
    COUNT(*) as n,
    ROUND(MIN(fill_price)::numeric, 4) as p_min,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fill_price)::numeric, 4) as p25,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fill_price)::numeric, 4) as p50,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fill_price)::numeric, 4) as p75,
    ROUND(MAX(fill_price)::numeric, 4) as p_max
FROM tick_data
WHERE mechanism = 'DETERMINISTIC'
GROUP BY symbol
ORDER BY n DESC;

-- 4. Batch analysis (determinism metric)
SELECT '=== BATCH ANALYSIS ===' as section;
SELECT
    COUNT(DISTINCT batch_id) as total_batches,
    ROUND(AVG(fills_per_batch)::numeric, 2) as avg_fills_per_batch,
    MAX(fills_per_batch) as max_fills_per_batch
FROM (
    SELECT batch_id, COUNT(*) as fills_per_batch
    FROM tick_data
    WHERE batch_id IS NOT NULL
    GROUP BY batch_id
) sub;


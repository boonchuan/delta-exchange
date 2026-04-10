#!/bin/bash
# Export research data to CSV for Python/R analysis

EXPORT_DIR="/opt/delta-exchange/research/exports"
mkdir -p $EXPORT_DIR
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "Exporting research data..."

# Full tick data
su - postgres -c "psql -d delta_exchange -c \"COPY (SELECT * FROM tick_data ORDER BY time) TO STDOUT WITH CSV HEADER\"" > $EXPORT_DIR/tick_data_${TIMESTAMP}.csv

# Fill data
su - postgres -c "psql -d delta_exchange -c \"COPY (SELECT * FROM fills ORDER BY created_at) TO STDOUT WITH CSV HEADER\"" > $EXPORT_DIR/fills_${TIMESTAMP}.csv

# Summary stats
su - postgres -c "psql -d delta_exchange -c \"COPY (SELECT symbol, mechanism, COUNT(*) as fills, AVG(fill_price) as avg_px, STDDEV(fill_price) as stddev_px, AVG(slippage) as avg_slip FROM tick_data GROUP BY symbol, mechanism ORDER BY symbol) TO STDOUT WITH CSV HEADER\"" > $EXPORT_DIR/summary_${TIMESTAMP}.csv

echo "Exported to $EXPORT_DIR/"
ls -lh $EXPORT_DIR/*${TIMESTAMP}*

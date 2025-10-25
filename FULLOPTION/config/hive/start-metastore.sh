#!/bin/bash
set -e

echo "👉 Init Hive schema nếu cần..."
/opt/hive/bin/schematool -dbType postgres -initSchema --verbose || true

echo "👉 Start Hive Metastore..."
/opt/hive/bin/hive --service metastore

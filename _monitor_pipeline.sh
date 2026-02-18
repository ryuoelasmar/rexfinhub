#!/bin/bash
# Monitor pipeline progress every 2 minutes
while true; do
  # Check if PID 34008 is still running
  if ! ps -W 2>/dev/null | grep -q "34008.*python"; then
    echo "$(date): Pipeline process (PID 34008) has STOPPED"
    echo "Final log:"
    cat -v _pipeline.log | tr '\r' '\n' | grep -v "^$" | tail -10
    echo "---"
    echo "Manifests created:"
    find outputs/ -name "_manifest.json" -newer _pipeline.log 2>/dev/null | wc -l
    echo "Step3 CSVs:"  
    find outputs/ -name "step3_*.csv" -newer _pipeline.log 2>/dev/null | wc -l
    echo "DB last modified:"
    ls -la data/etp_tracker.db 2>/dev/null
    break
  fi
  
  CACHE_COUNT=$(find http_cache/ -type f -newer _pipeline.log 2>/dev/null | wc -l)
  MANIFEST_COUNT=$(find outputs/ -name "_manifest.json" -newer _pipeline.log 2>/dev/null | wc -l)
  echo "$(date): Pipeline running. Downloads: $CACHE_COUNT | Manifests: $MANIFEST_COUNT"
  sleep 120
done
echo "$(date): Monitoring complete."

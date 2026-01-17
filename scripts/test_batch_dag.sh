#!/bin/bash
# Test script for running Spark Batch DAG with 2026-01-11 data
# This script:
# 1. Cleans existing PostgreSQL data for 2026-01-11
# 2. Triggers Airflow DAG manually
# 3. Monitors execution status


# bash test_batch_dag.sh


set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TARGET_DATE="2026-01-11"
DAG_ID="spark_batch_processing"
EXECUTION_DATE="2026-01-12T03:00:00+00:00"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Spark Batch DAG Test Script${NC}"
echo -e "${BLUE}Target Date: ${TARGET_DATE}${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Step 1: Clean PostgreSQL data
echo -e "${YELLOW}[Step 1/4] Cleaning PostgreSQL data for ${TARGET_DATE}...${NC}"

docker exec postgres psql -U admin -d clickstream <<EOF
-- Delete existing data for target date
DELETE FROM daily_metrics WHERE metric_date = '${TARGET_DATE}';
DELETE FROM popular_items WHERE metric_date = '${TARGET_DATE}';
DELETE FROM popular_categories WHERE metric_date = '${TARGET_DATE}';
DELETE FROM session_funnel WHERE metric_date = '${TARGET_DATE}';

-- Show deletion results
SELECT 'Data cleaned for ${TARGET_DATE}' AS status;
EOF

echo -e "${GREEN}✓ PostgreSQL data cleaned${NC}"
echo ""

# Step 2: Unpause DAG and wait for parsing
echo -e "${YELLOW}[Step 2/4] Ensuring DAG is active...${NC}"

# Unpause the DAG
docker exec airflow-scheduler airflow dags unpause ${DAG_ID} 2>/dev/null || echo "DAG already unpaused or not yet parsed"

# Wait for DAG to be parsed
echo -e "${BLUE}Waiting for DAG to be parsed by scheduler...${NC}"
sleep 5

echo -e "${GREEN}✓ DAG is ready${NC}"
echo ""

# Step 3: Trigger DAG manually
echo -e "${YELLOW}[Step 3/4] Triggering DAG: ${DAG_ID}...${NC}"

docker exec airflow-scheduler airflow dags trigger ${DAG_ID} \
  --exec-date ${EXECUTION_DATE} \
  --conf '{"target_date": "'${TARGET_DATE}'"}'

if [ $? -ne 0 ]; then
  echo -e "${RED}✗ Failed to trigger DAG${NC}"
  exit 1
fi

echo -e "${GREEN}✓ DAG triggered successfully${NC}"
echo -e "  Execution Date: ${EXECUTION_DATE}"
echo ""

# Step 4: Monitor execution
echo -e "${YELLOW}[Step 4/4] Monitoring DAG execution...${NC}"
echo -e "${BLUE}Waiting for tasks to complete (checking every 10 seconds)${NC}"
echo ""

MAX_WAIT=600  # 10 minutes
ELAPSED=0
INTERVAL=10

while [ $ELAPSED -lt $MAX_WAIT ]; do
  # Get DAG run state
  STATE=$(docker exec airflow-scheduler airflow dags list-runs -d ${DAG_ID} 2>/dev/null | grep "manual__${EXECUTION_DATE}" | awk '{print $4}' | head -1)
  
  if [ "$STATE" == "success" ]; then
    echo -e "${GREEN}✓ DAG execution completed successfully!${NC}"
    echo ""
    break
  elif [ "$STATE" == "failed" ]; then
    echo -e "${RED}✗ DAG execution failed!${NC}"
    echo ""
    echo -e "${YELLOW}Task statuses:${NC}"
    docker exec airflow-scheduler airflow tasks states-for-dag-run ${DAG_ID} manual__${EXECUTION_DATE}
    exit 1
  elif [ "$STATE" == "running" ] || [ "$STATE" == "queued" ]; then
    # Show task progress
    TASK_STATES=$(docker exec airflow-scheduler airflow tasks states-for-dag-run ${DAG_ID} manual__${EXECUTION_DATE} 2>/dev/null | tail -n +2)
    echo -e "${BLUE}[$(date +'%H:%M:%S')] Running... ($((ELAPSED))s elapsed)${NC}"
    echo "$TASK_STATES" | while read line; do
      TASK=$(echo $line | awk '{print $1}')
      STATUS=$(echo $line | awk '{print $NF}')
      
      if [ "$STATUS" == "success" ]; then
        echo -e "  ${GREEN}✓${NC} $TASK: $STATUS"
      elif [ "$STATUS" == "running" ]; then
        echo -e "  ${YELLOW}⟳${NC} $TASK: $STATUS"
      elif [ "$STATUS" == "failed" ]; then
        echo -e "  ${RED}✗${NC} $TASK: $STATUS"
      else
        echo -e "  ${BLUE}○${NC} $TASK: $STATUS"
      fi
    done
    echo ""
  else
    echo -e "${BLUE}[$(date +'%H:%M:%S')] Waiting for DAG to start... ($((ELAPSED))s elapsed)${NC}
  
  sleep $INTERVAL
  ELAPSED=$((ELAPSED + INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
  echo -e "${RED}✗ Timeout waiting for DAG completion (${MAX_WAIT}s)${NC}"
  exit 1
fi

# Step 5: Validate results
echo -e "${YELLOW}[Validation] Checking PostgreSQL data...${NC}"

docker exec postgres psql -U admin -d clickstream <<EOF
-- Count records for each table
SELECT 
  'daily_metrics' as table_name, 
  COUNT(*) as count 
FROM daily_metrics 
WHERE metric_date = '${TARGET_DATE}'

UNION ALL

SELECT 
  'popular_items' as table_name, 
  COUNT(*) as count 
FROM popular_items 
WHERE metric_date = '${TARGET_DATE}'

UNION ALL

SELECT 
  'popular_categories' as table_name, 
  COUNT(*) as count 
FROM popular_categories 
WHERE metric_date = '${TARGET_DATE}'

UNION ALL

SELECT 
  'session_funnel' as table_name, 
  COUNT(*) as count 
FROM session_funnel 
WHERE metric_date = '${TARGET_DATE}';
EOF

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Test completed successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo -e "  1. View logs: docker exec airflow-scheduler airflow tasks log ${DAG_ID} <task_id> manual__${EXECUTION_DATE}"
echo -e "  2. Check Airflow UI: http://localhost:8080"
echo -e "  3. Query data: docker exec postgres psql -U admin -d clickstream"
echo ""

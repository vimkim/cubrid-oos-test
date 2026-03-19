#!/bin/bash
#
# Common helper functions for OOS shell tests.
#
# Inspired by CTP shell/init_path/init.sh.
# Source this from each test script after setting DB_NAME and LOG_FILE.
#
# Required variables (set before sourcing):
#   DB_NAME   - database name
#   LOG_FILE  - log file path
#
# Optional variables:
#   DB_VOL_PATH       - volume directory (default: ${CUBRID_DATABASES:-/tmp}/$DB_NAME)
#   DB_VOL_SIZE       - createdb volume size (default: 512M)
#   DB_LOG_SIZE       - createdb log volume size (default: 256M)
#

set -u

# ============================================================================
# Counters
# ============================================================================

PASS_COUNT=0
FAIL_COUNT=0

# ============================================================================
# Logging
# ============================================================================

log_msg() {
    echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# ============================================================================
# SQL execution
# ============================================================================

run_sql() {
    csql -u dba "$DB_NAME" -c "$1" 2>&1
}

run_sql_file() {
    csql -u dba "$DB_NAME" -i "$1" 2>&1
}

# ============================================================================
# Assertions
# ============================================================================

assert_contains() {
    local desc="$1"
    local expected_substr="$2"
    local actual="$3"

    if echo "$actual" | grep -q "$expected_substr"; then
        log_msg "PASS: $desc"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        log_msg "FAIL: $desc (expected to contain '$expected_substr', got '$actual')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
}

assert_not_contains() {
    local desc="$1"
    local unexpected_substr="$2"
    local actual="$3"

    if echo "$actual" | grep -q "$unexpected_substr"; then
        log_msg "FAIL: $desc (should NOT contain '$unexpected_substr', but got '$actual')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    else
        log_msg "PASS: $desc"
        PASS_COUNT=$((PASS_COUNT + 1))
    fi
}

assert_equals() {
    local desc="$1"
    local expected="$2"
    local actual="$3"

    if [ "$expected" = "$actual" ]; then
        log_msg "PASS: $desc"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        log_msg "FAIL: $desc (expected='$expected', actual='$actual')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
}

# ============================================================================
# Database lifecycle (for tests that create their own DB)
# ============================================================================

: "${DB_VOL_PATH:=${CUBRID_DATABASES:-/tmp}/${DB_NAME}}"
: "${DB_VOL_SIZE:=512M}"
: "${DB_LOG_SIZE:=256M}"

create_db() {
    log_msg "Creating database $DB_NAME..."
    mkdir -p "$DB_VOL_PATH"
    cubrid createdb --db-volume-size="$DB_VOL_SIZE" --log-volume-size="$DB_LOG_SIZE" \
        "$DB_NAME" en_US.utf8 -F "$DB_VOL_PATH" 2>&1 | tee -a "$LOG_FILE"
}

start_server() {
    log_msg "Starting server for $DB_NAME..."
    cubrid server start "$DB_NAME" 2>&1 | tee -a "$LOG_FILE"
    cubrid broker start 2>&1 | tee -a "$LOG_FILE"
    sleep 2
}

stop_server() {
    log_msg "Stopping server for $DB_NAME..."
    cubrid broker stop 2>&1 | tee -a "$LOG_FILE"
    cubrid server stop "$DB_NAME" 2>&1 | tee -a "$LOG_FILE"
    sleep 1
}

kill_server() {
    log_msg "KILLING server for $DB_NAME (simulating crash)..."
    local pid
    pid=$(ps aux | grep "cub_server $DB_NAME" | grep -v grep | awk '{print $2}')
    if [ -n "$pid" ]; then
        kill -9 "$pid" 2>/dev/null
        sleep 2
        log_msg "Killed server PID=$pid"
    else
        log_msg "WARNING: Could not find server process to kill"
    fi
}

cleanup_db() {
    cubrid broker stop 2>/dev/null
    cubrid server stop "$DB_NAME" 2>/dev/null
    sleep 1
    cubrid deletedb "$DB_NAME" 2>/dev/null
    rm -rf "$DB_VOL_PATH" 2>/dev/null
    rm -rf "${DB_NAME}"* 2>/dev/null
    local db_txt="${CUBRID_DATABASES:-/tmp}/databases.txt"
    if [ -f "$db_txt" ]; then
        sed -i "/^${DB_NAME}[[:space:]]/d" "$db_txt"
    fi
    mkdir -p "$DB_VOL_PATH"
}

# ============================================================================
# Results summary - call at the end of main()
# ============================================================================

print_results() {
    log_msg "======================================"
    log_msg "Results: PASS=$PASS_COUNT, FAIL=$FAIL_COUNT"
    log_msg "======================================"

    if [ "$FAIL_COUNT" -gt 0 ]; then
        exit 1
    fi
    exit 0
}

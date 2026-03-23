#!/bin/bash
#
# OOS Physical Delete Validation Tests
#
# CBRD-26609: Validate oos_delete API through SQL-level DELETE operations.
# oos_delete is invoked by vacuum after heap records are committed-deleted.
# These tests verify data integrity, space reuse, rollback safety, and
# vacuum-driven OOS cleanup via observable SQL behavior.
#
# Prerequisites:
#   - CUBRID installed and in PATH
#   - csql command available
#
# Usage: bash oos_delete_physical.sh [db_name]
#

DB_NAME="${1:-oos_del_phys_test}"
LOG_FILE="oos_delete_phys_$(date +%Y%m%d_%H%M%S).log"
DB_VOL_SIZE="512M"
DB_LOG_SIZE="256M"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

# ============================================================================
# TC-01: DELETE single-chunk then verify remaining rows
# ============================================================================

test_delete_single_chunk() {
    log_msg "=== TC-01: DELETE single-chunk OOS record ==="

    run_sql "DROP TABLE IF EXISTS t_del_sc;"
    run_sql "CREATE TABLE t_del_sc (
        id INT PRIMARY KEY,
        data_col BIT VARYING
    );"

    run_sql "INSERT INTO t_del_sc VALUES (1, REPEAT(X'AA', 1024));"
    run_sql "INSERT INTO t_del_sc VALUES (2, REPEAT(X'BB', 2048));"
    run_sql "INSERT INTO t_del_sc VALUES (3, REPEAT(X'CC', 1024));"

    run_sql "DELETE FROM t_del_sc WHERE id = 2;"

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_sc;")
    assert_contains "Row count after delete" "2" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_sc ORDER BY id;")
    assert_contains "Row 1 intact" "1024" "$result"
    assert_not_contains "Deleted row gone" "2048" "$result"

    run_sql "DROP TABLE t_del_sc;"
    log_msg "=== TC-01 complete ==="
}

# ============================================================================
# TC-02: DELETE multi-chunk OOS record (64KB = multiple OOS pages)
# ============================================================================

test_delete_multi_chunk() {
    log_msg "=== TC-02: DELETE multi-chunk OOS record ==="

    run_sql "DROP TABLE IF EXISTS t_del_mc;"
    run_sql "CREATE TABLE t_del_mc (
        id INT PRIMARY KEY,
        big_col BIT VARYING
    );"

    run_sql "INSERT INTO t_del_mc VALUES (1, REPEAT(X'AA', 32768));"
    run_sql "INSERT INTO t_del_mc VALUES (2, REPEAT(X'BB', 65536));"
    run_sql "INSERT INTO t_del_mc VALUES (3, REPEAT(X'CC', 32768));"

    run_sql "DELETE FROM t_del_mc WHERE id = 2;"

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_mc;")
    assert_contains "Row count after multi-chunk delete" "2" "$result"

    result=$(run_sql "SELECT id, LENGTH(big_col) FROM t_del_mc WHERE id = 1;")
    assert_contains "Row 1 len intact" "32768" "$result"

    result=$(run_sql "SELECT id, LENGTH(big_col) FROM t_del_mc WHERE id = 3;")
    assert_contains "Row 3 len intact" "32768" "$result"

    run_sql "DROP TABLE t_del_mc;"
    log_msg "=== TC-02 complete ==="
}

# ============================================================================
# TC-03: DELETE 160KB record (long chunk chain)
# ============================================================================

test_delete_large_chain() {
    log_msg "=== TC-03: DELETE 160KB multi-chunk record ==="

    run_sql "DROP TABLE IF EXISTS t_del_large;"
    run_sql "CREATE TABLE t_del_large (
        id INT PRIMARY KEY,
        huge_col BIT VARYING
    );"

    run_sql "INSERT INTO t_del_large VALUES (1, REPEAT(X'AA', 163840));"
    run_sql "INSERT INTO t_del_large VALUES (2, REPEAT(X'BB', 1024));"

    local result
    result=$(run_sql "SELECT id, LENGTH(huge_col) FROM t_del_large WHERE id = 1;")
    assert_contains "160KB row exists" "163840" "$result"

    run_sql "DELETE FROM t_del_large WHERE id = 1;"

    result=$(run_sql "SELECT COUNT(*) FROM t_del_large;")
    assert_contains "Only small row remains" "1" "$result"

    result=$(run_sql "SELECT id, LENGTH(huge_col) FROM t_del_large WHERE id = 2;")
    assert_contains "Small row intact" "1024" "$result"

    run_sql "DROP TABLE t_del_large;"
    log_msg "=== TC-03 complete ==="
}

# ============================================================================
# TC-04: Bulk DELETE (many OOS records at once)
# ============================================================================

test_bulk_delete() {
    log_msg "=== TC-04: Bulk DELETE 200 OOS records ==="

    run_sql "DROP TABLE IF EXISTS t_del_bulk;"
    run_sql "CREATE TABLE t_del_bulk (
        id INT PRIMARY KEY AUTO_INCREMENT,
        data_col BIT VARYING
    );"

    run_sql "INSERT INTO t_del_bulk (data_col)
        SELECT REPEAT(X'AA', 1024 + (ROWNUM * 4))
        FROM db_class a, db_class b
        LIMIT 200;"

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_bulk;")
    assert_contains "Pre-delete count" "200" "$result"

    run_sql "DELETE FROM t_del_bulk;"

    result=$(run_sql "SELECT COUNT(*) FROM t_del_bulk;")
    assert_contains "Post-delete count" "0" "$result"

    run_sql "DROP TABLE t_del_bulk;"
    log_msg "=== TC-04 complete ==="
}

# ============================================================================
# TC-05: DELETE then reinsert (space reuse after vacuum)
# ============================================================================

test_delete_reinsert() {
    log_msg "=== TC-05: DELETE all then reinsert ==="

    run_sql "DROP TABLE IF EXISTS t_del_reins;"
    run_sql "CREATE TABLE t_del_reins (
        id INT PRIMARY KEY,
        data_col BIT VARYING
    );"

    run_sql "INSERT INTO t_del_reins VALUES (1, REPEAT(X'AA', 4096));"
    run_sql "INSERT INTO t_del_reins VALUES (2, REPEAT(X'BB', 8192));"
    run_sql "INSERT INTO t_del_reins VALUES (3, REPEAT(X'CC', 16384));"

    run_sql "DELETE FROM t_del_reins;"

    # wait for vacuum to reclaim OOS pages
    sleep 5

    # reinsert into same table
    run_sql "INSERT INTO t_del_reins VALUES (10, REPEAT(X'DD', 4096));"
    run_sql "INSERT INTO t_del_reins VALUES (20, REPEAT(X'EE', 8192));"

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_reins;")
    assert_contains "Reinsert count" "2" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_reins WHERE id = 10;")
    assert_contains "Reinserted row 10 length" "4096" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_reins WHERE id = 20;")
    assert_contains "Reinserted row 20 length" "8192" "$result"

    run_sql "DROP TABLE t_del_reins;"
    log_msg "=== TC-05 complete ==="
}

# ============================================================================
# TC-06: Repeated DELETE + INSERT cycles (stress OOS alloc/dealloc)
# ============================================================================

test_delete_insert_cycles() {
    log_msg "=== TC-06: 20 DELETE+INSERT cycles ==="

    run_sql "DROP TABLE IF EXISTS t_del_cycle;"
    run_sql "CREATE TABLE t_del_cycle (
        id INT PRIMARY KEY,
        data_col BIT VARYING
    );"

    local i
    for i in $(seq 1 20); do
        local hex_byte
        hex_byte=$(printf '%02x' $((i % 256)))
        local size=$((1024 + i * 512))
        run_sql "INSERT INTO t_del_cycle VALUES ($i, REPEAT(X'${hex_byte}', $size));" > /dev/null
        run_sql "DELETE FROM t_del_cycle WHERE id = $i;" > /dev/null
    done

    # final insert - keep it
    run_sql "INSERT INTO t_del_cycle VALUES (999, REPEAT(X'FF', 4096));"

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_cycle;")
    assert_contains "Only final row remains" "1" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_cycle WHERE id = 999;")
    assert_contains "Final row length" "4096" "$result"

    run_sql "DROP TABLE t_del_cycle;"
    log_msg "=== TC-06 complete ==="
}

# ============================================================================
# TC-07: ROLLBACK after DELETE (OOS data must survive)
# ============================================================================

test_rollback_preserves_oos() {
    log_msg "=== TC-07: ROLLBACK preserves OOS data ==="

    run_sql "DROP TABLE IF EXISTS t_del_rb;"
    run_sql "CREATE TABLE t_del_rb (
        id INT PRIMARY KEY,
        data_col BIT VARYING
    );"

    run_sql "INSERT INTO t_del_rb VALUES (1, REPEAT(X'AA', 2048));"
    run_sql "INSERT INTO t_del_rb VALUES (2, REPEAT(X'BB', 4096));"
    run_sql "COMMIT;"

    # delete + rollback via a single csql session using -c with semicolons
    # csql -c runs in autocommit off when multiple statements
    run_sql_file <(cat <<'EOSQL'
DELETE FROM t_del_rb WHERE id = 1;
ROLLBACK;
EOSQL
)

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_rb;")
    assert_contains "Both rows survive rollback" "2" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_rb WHERE id = 1;")
    assert_contains "Row 1 OOS data intact after rollback" "2048" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_rb WHERE id = 2;")
    assert_contains "Row 2 OOS data intact after rollback" "4096" "$result"

    run_sql "DROP TABLE t_del_rb;"
    log_msg "=== TC-07 complete ==="
}

# ============================================================================
# TC-08: UPDATE then DELETE (both oos_delete code paths)
# ============================================================================

test_update_then_delete() {
    log_msg "=== TC-08: UPDATE then DELETE ==="

    run_sql "DROP TABLE IF EXISTS t_upd_del;"
    run_sql "CREATE TABLE t_upd_del (
        id INT PRIMARY KEY,
        data_col BIT VARYING
    );"

    run_sql "INSERT INTO t_upd_del VALUES (1, REPEAT(X'AA', 2048));"

    # UPDATE replaces old OOS with new OOS (old OOS deleted)
    run_sql "UPDATE t_upd_del SET data_col = REPEAT(X'BB', 4096) WHERE id = 1;"

    local result
    result=$(run_sql "SELECT LENGTH(data_col) FROM t_upd_del WHERE id = 1;")
    assert_contains "After update: new OOS length" "4096" "$result"

    # DELETE the row (new OOS cleaned by vacuum)
    run_sql "DELETE FROM t_upd_del WHERE id = 1;"

    result=$(run_sql "SELECT COUNT(*) FROM t_upd_del;")
    assert_contains "No rows after delete" "0" "$result"

    run_sql "DROP TABLE t_upd_del;"
    log_msg "=== TC-08 complete ==="
}

# ============================================================================
# TC-09: Mixed OOS / non-OOS row deletes
# ============================================================================

test_mixed_oos_delete() {
    log_msg "=== TC-09: Mixed OOS and non-OOS deletes ==="

    run_sql "DROP TABLE IF EXISTS t_del_mix;"
    run_sql "CREATE TABLE t_del_mix (
        id INT PRIMARY KEY,
        data_col BIT VARYING
    );"

    # non-OOS (below threshold)
    run_sql "INSERT INTO t_del_mix VALUES (1, REPEAT(X'AA', 100));"
    run_sql "INSERT INTO t_del_mix VALUES (2, REPEAT(X'BB', 200));"
    # OOS (above threshold)
    run_sql "INSERT INTO t_del_mix VALUES (3, REPEAT(X'CC', 2048));"
    run_sql "INSERT INTO t_del_mix VALUES (4, REPEAT(X'DD', 4096));"

    # delete one of each
    run_sql "DELETE FROM t_del_mix WHERE id IN (1, 3);"

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_mix;")
    assert_contains "2 rows remain" "2" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_mix WHERE id = 2;")
    assert_contains "Non-OOS row 2 intact" "200" "$result"

    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_mix WHERE id = 4;")
    assert_contains "OOS row 4 intact" "4096" "$result"

    run_sql "DROP TABLE t_del_mix;"
    log_msg "=== TC-09 complete ==="
}

# ============================================================================
# TC-10: Selective delete by predicate on many OOS rows
# ============================================================================

test_selective_predicate_delete() {
    log_msg "=== TC-10: Selective delete by predicate ==="

    run_sql "DROP TABLE IF EXISTS t_del_pred;"
    run_sql "CREATE TABLE t_del_pred (
        id INT PRIMARY KEY,
        grp INT,
        data_col BIT VARYING
    );"

    # insert 50 rows in 2 groups
    local i
    for i in $(seq 1 50); do
        local grp=$((i % 2))
        local hex_byte=$(printf '%02x' $((i % 256)))
        run_sql "INSERT INTO t_del_pred VALUES ($i, $grp, REPEAT(X'${hex_byte}', $((1024 + i * 100))));" > /dev/null
    done

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_pred;")
    assert_contains "Pre-delete total" "50" "$result"

    # delete group 0 (even ids: 2,4,...,50 = 25 rows)
    run_sql "DELETE FROM t_del_pred WHERE grp = 0;"

    result=$(run_sql "SELECT COUNT(*) FROM t_del_pred;")
    assert_contains "Group 1 rows remain" "25" "$result"

    # verify data integrity of remaining rows
    result=$(run_sql "SELECT COUNT(*) FROM t_del_pred WHERE grp = 1;")
    assert_contains "All remaining are group 1" "25" "$result"

    # spot check a remaining row
    result=$(run_sql "SELECT id, LENGTH(data_col) FROM t_del_pred WHERE id = 1;")
    assert_contains "Row 1 length" "1124" "$result"

    run_sql "DROP TABLE t_del_pred;"
    log_msg "=== TC-10 complete ==="
}

# ============================================================================
# TC-11: DELETE + vacuum + reinsert same PK
# ============================================================================

test_delete_reinsert_same_pk() {
    log_msg "=== TC-11: DELETE + reinsert same PK ==="

    run_sql "DROP TABLE IF EXISTS t_del_samepk;"
    run_sql "CREATE TABLE t_del_samepk (
        id INT PRIMARY KEY,
        data_col BIT VARYING
    );"

    run_sql "INSERT INTO t_del_samepk VALUES (1, REPEAT(X'AA', 4096));"

    local result
    result=$(run_sql "SELECT LENGTH(data_col) FROM t_del_samepk WHERE id = 1;")
    assert_contains "Original length" "4096" "$result"

    run_sql "DELETE FROM t_del_samepk WHERE id = 1;"

    # reinsert same PK, different data
    run_sql "INSERT INTO t_del_samepk VALUES (1, REPEAT(X'FF', 8192));"

    result=$(run_sql "SELECT LENGTH(data_col) FROM t_del_samepk WHERE id = 1;")
    assert_contains "New data length" "8192" "$result"

    run_sql "DROP TABLE t_del_samepk;"
    log_msg "=== TC-11 complete ==="
}

# ============================================================================
# TC-12: Concurrent DELETE sessions
# ============================================================================

test_concurrent_deletes() {
    log_msg "=== TC-12: Concurrent DELETE sessions ==="

    run_sql "DROP TABLE IF EXISTS t_del_conc;"
    run_sql "CREATE TABLE t_del_conc (
        id INT PRIMARY KEY,
        grp INT,
        data_col BIT VARYING
    );"

    # insert 100 rows in 5 groups
    local i
    for i in $(seq 1 100); do
        local grp=$(( ((i - 1) / 20) + 1 ))
        run_sql "INSERT INTO t_del_conc VALUES ($i, $grp, REPEAT(X'AA', 1024));" > /dev/null
    done

    local result
    result=$(run_sql "SELECT COUNT(*) FROM t_del_conc;")
    assert_contains "Pre-concurrent-delete count" "100" "$result"

    # launch 5 concurrent delete sessions, each deleting its own group
    for grp in $(seq 1 5); do
        (
            run_sql "DELETE FROM t_del_conc WHERE grp = $grp;" > /dev/null
        ) &
    done

    wait

    result=$(run_sql "SELECT COUNT(*) FROM t_del_conc;")
    assert_contains "All rows deleted concurrently" "0" "$result"

    run_sql "DROP TABLE t_del_conc;"
    log_msg "=== TC-12 complete ==="
}

# ============================================================================
# Main
# ============================================================================

main() {
    log_msg "======================================"
    log_msg "OOS Physical Delete Test Suite (CBRD-26609)"
    log_msg "======================================"

    cleanup_db
    create_db
    start_server

    test_delete_single_chunk
    test_delete_multi_chunk
    test_delete_large_chain
    test_bulk_delete
    test_delete_reinsert
    test_delete_insert_cycles
    test_rollback_preserves_oos
    test_update_then_delete
    test_mixed_oos_delete
    test_selective_predicate_delete
    test_delete_reinsert_same_pk
    test_concurrent_deletes

    stop_server
    cleanup_db

    print_results
}

main "$@"

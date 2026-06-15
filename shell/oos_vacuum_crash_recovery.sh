#!/bin/bash
#
# OOS Vacuum + Crash Recovery Tests   (CBRD-26668)
#
# Answers reviewer question on PR #6986 / vacuum_heap_record:
#
#   "oos delete - sysop commit ~~ crash ~~ bulk vacuum heap 순서로
#    heap vacuum 전 crash 발생 시 oos는 commit 처리되어 제거된 상태일 것 같습니다.
#    혹시 이 부분에 대해 복구 검증 진행 되었나요?"
#
#   ("If a crash happens after the OOS delete is committed but before the heap
#    record is vacuumed, the OOS would already be gone while the heap record
#    still references it. Has recovery been verified for that?")
#
# DESIGN ANSWER (what this test exercises end-to-end through the real server):
#
#   vacuum_heap_record() removes an OOS-bearing dead record inside ONE system
#   operation:
#
#       log_sysop_start
#         vacuum_log_redoundo_vacuum_record   <- heap slot removal (logged)
#         vacuum_heap_oos_delete_within_sysop  <- OOS chunk deletes  (logged)
#       log_sysop_commit
#
#   The heap-slot removal and the OOS deletes therefore share a single atomic,
#   durable WAL unit. The "OOS committed, heap not yet vacuumed" ordering the
#   reviewer worried about cannot occur: a crash either REDOES the whole sysop
#   (heap slot gone AND OOS chunks gone) or rolls it ALL back (heap record AND
#   its OOS chunks both intact). Never torn. See
#   docs/adr/0001-synchronous-oos-reclaim-in-vacuum-sysop.md.
#
# These tests prove that invariant against a real crash: they drive the real
# vacuum pipeline to reclaim OOS-bearing dead records, kill -9 the server while
# vacuum is working, restart (forcing WAL recovery), and assert the database is
# consistent for ANY crash timing:
#
#   * no OOS leak           -> ';oos_stats' Live OOS records drains to expected
#   * no dangling reference -> surviving rows still read their exact value
#   * structural integrity  -> cubrid checkdb is clean
#   * vacuum still drains    -> a post-recovery vacuum finishes cleanly
#
# Observability: the ';oos_stats <class>' csql metacommand prints
# "Live OOS records : N" (every chunk of every chain counted) — the shell
# equivalent of the unit test's oos_live_recs() in
# unit_tests/oos/test_oos_real_vacuum_server.cpp.
#
# Prerequisites: a DEBUG build of CUBRID in PATH (cubrid, csql), and the
# ;oos_stats metacommand (present on the oos-vacuum branch).
#
# Usage: bash oos_vacuum_crash_recovery.sh
#

# NB: CUBRID requires the DB-name log prefix to be < 17 chars.
DB_NAME="oosvaccrashdb"
LOG_FILE="oos_vacuum_crash_recovery_$(date +%Y%m%d_%H%M%S).log"

# Modest volumes (set before sourcing common.sh, which honours them via :=).
# 512M keeps crash-recovery startup fast (~2s); a larger volume measurably
# slows the post-crash restart. The scenarios no longer DROP TABLE (teardown is
# cleanup_db), so there is no temp-volume spike to size up for.
: "${DB_VOL_SIZE:=512M}"
: "${DB_LOG_SIZE:=256M}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

# Number of OOS rows. Larger => more OOS records => more vacuum sysops => higher
# chance the kill -9 lands *mid*-vacuum (the interesting window). Correctness is
# asserted for every crash timing regardless.
: "${N_SMALL_ROWS:=50}"          # single-chunk 4KB OOS rows
: "${N_BIG_ROWS:=5}"             # multi-chunk (256KB) OOS rows -> chain reclaim coverage
: "${SMALL_BYTES:=4096}"
: "${BIG_BYTES:=262144}"

: "${DRAIN_TIMEOUT:=120}"        # seconds to wait for a full post-recovery vacuum drain
: "${RECLAIM_WATCH_TIMEOUT:=12}" # seconds to watch for vacuum to *start* before crashing

# ============================================================================
# Helpers (build on lib/common.sh: run_sql, kill_server, start_server, asserts)
# ============================================================================

# common.sh start_server only sleeps 2s; on a debug build (or after crash
# recovery) the server can take longer to accept connections. Poll until a
# trivial query succeeds so the first DDL never races a not-ready server.
wait_for_server_ready() {
    local timeout="${1:-40}"
    local deadline=$(( $(date +%s) + timeout ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if csql -u dba "$DB_NAME" -c "SELECT 1;" 2>/dev/null \
            | grep -qE '^[[:space:]]*1[[:space:]]*$'; then
            return 0
        fi
        sleep 1
    done
    log_msg "WARNING: server for $DB_NAME not ready after ${timeout}s"
    return 1
}

# start_server (lifecycle) + readiness gate. Use everywhere instead of bare
# start_server so recovery time after a crash is waited out, not guessed.
start_server_ready() {
    start_server
    wait_for_server_ready 120
}

# --- Broker-free lifecycle overrides (shadow lib/common.sh) -----------------
# csql connects DIRECTLY to cub_server, so this test needs no CAS broker. We
# deliberately do NOT touch the broker because:
#   (1) 'cubrid broker start|stop' is GLOBAL — it would disturb any co-tenant
#       CUBRID servers sharing this host;
#   (2) piping a broker start through 'tee' can HANG: the CAS daemons inherit
#       the pipe fd and never close it, so the pipeline never sees EOF.
# These overrides are server-only and redirect to the log via a file (not a
# pipe), so no daemon fd-inheritance can wedge them.
start_server() {
    log_msg "Starting server for $DB_NAME (no broker)..."
    cubrid server start "$DB_NAME" >>"$LOG_FILE" 2>&1
}

stop_server() {
    log_msg "Stopping server for $DB_NAME..."
    cubrid server stop "$DB_NAME" >>"$LOG_FILE" 2>&1
}

cleanup_db() {
    cubrid server stop "$DB_NAME" >/dev/null 2>&1
    cubrid deletedb "$DB_NAME" >/dev/null 2>&1
    rm -rf "$DB_VOL_PATH" >/dev/null 2>&1
    local db_txt="${CUBRID_DATABASES:-/tmp}/databases.txt"
    [ -f "$db_txt" ] && sed -i "/^${DB_NAME}[[:space:]]/d" "$db_txt"
    mkdir -p "$DB_VOL_PATH"
}

# Extract the single scalar value printed by a "SELECT <agg>" csql result.
# Echoes the integer, or -1 if the query errored / produced no scalar.
scalar_query() {
    local sql="$1"
    local out
    out=$(run_sql "$sql")
    if echo "$out" | grep -qi "ERROR"; then
        echo "-1"
        return
    fi
    # The value row is the only line that is purely digits + surrounding spaces.
    echo "$out" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]'
}

count_rows() {
    scalar_query "SELECT COUNT(*) FROM $1;"
}

# Rows whose OOS column exactly equals REPEAT(X'<hex>', <nbytes>) — i.e. value
# equality, the dangling-reference detector.
value_match_count() {
    local table="$1" col="$2" hex="$3" nbytes="$4"
    scalar_query \
        "SELECT COUNT(*) FROM $table WHERE $col = CAST(REPEAT(X'$hex', $nbytes) AS BIT VARYING);"
}

# Live OOS record count for a class via the ;oos_stats metacommand.
# Echoes the integer, 0 if the class has no OOS file, or -1 on error.
oos_live_recs() {
    local table="$1" out
    out=$(printf ';oos_stats %s\n' "$table" | csql -u dba "$DB_NAME" 2>&1)
    if echo "$out" | grep -q "has no OOS file"; then
        echo "0"
        return
    fi
    local n
    n=$(echo "$out" | sed -n 's/.*Live OOS records[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -1)
    if [ -z "$n" ]; then echo "-1"; else echo "$n"; fi
}

# Wake the vacuum master daemon (same entry point csql ';vacuum' uses).
nudge_vacuum() {
    printf ';vacuum\n' | csql -u dba "$DB_NAME" >/dev/null 2>&1
}

# Advance the WAL so the log block holding our delete/update closes — vacuum
# only processes a block once it is closed and the append head has moved past
# it. A handful of committed filler transactions does that.
gen_filler() {
    local rounds="${1:-40}" i
    run_sql "CREATE TABLE IF NOT EXISTS t_filler (id INT PRIMARY KEY, pad BIT VARYING(40000));" >/dev/null 2>&1
    local sql=""
    for ((i = 0; i < rounds; i++)); do
        sql+="INSERT INTO t_filler VALUES ($i, REPEAT(X'5A', 2048));"
        sql+="DELETE FROM t_filler WHERE id = $i;"
    done
    printf '%s\n' "$sql" | csql -u dba "$DB_NAME" >/dev/null 2>&1
}

# Drive vacuum and wait until oos_live_recs(table) reaches <target>.
# Returns 0 on success, 1 on timeout. Used post-recovery to assert full drain.
drain_oos() {
    local table="$1" target="$2" timeout="${3:-$DRAIN_TIMEOUT}"
    local deadline=$(( $(date +%s) + timeout ))
    local live
    while [ "$(date +%s)" -lt "$deadline" ]; do
        gen_filler 20
        nudge_vacuum
        live=$(oos_live_recs "$table")
        if [ "$live" = "$target" ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

run_checkdb() {
    local out
    out=$(cubrid checkdb "$DB_NAME" 2>&1)
    assert_not_contains "checkdb is clean (no corruption)" "ERROR" "$out"
}

# Drive vacuum, then kill -9 as soon as reclaim is observed to have STARTED
# (live count drops below baseline) — biasing the crash to land mid-vacuum.
# Falls back to crashing anyway after RECLAIM_WATCH_TIMEOUT (covers the
# "vacuum still pending at crash" case, which recovery + a later vacuum must
# also survive).
crash_during_vacuum() {
    local table="$1" baseline="$2"
    local deadline=$(( $(date +%s) + RECLAIM_WATCH_TIMEOUT ))
    local live
    log_msg "Driving vacuum (baseline live OOS=$baseline), will crash on first reclaim..."
    while [ "$(date +%s)" -lt "$deadline" ]; do
        gen_filler 30
        nudge_vacuum
        live=$(oos_live_recs "$table")
        if [ "$live" != "-1" ] && [ "$live" -lt "$baseline" ]; then
            log_msg "Reclaim started (live OOS=$live < $baseline) -> crashing NOW"
            break
        fi
    done
    kill_server
}

# Insert <count> OOS rows into <table> starting at <start_id>, each column =
# REPEAT(X'<hex>', <nbytes>). Batched to minimize csql client spawns.
insert_oos_rows() {
    local table="$1" start_id="$2" count="$3" hex="$4" nbytes="$5"
    local i sql=""
    for ((i = 0; i < count; i++)); do
        sql+="INSERT INTO $table VALUES ($((start_id + i)), CAST(REPEAT(X'$hex', $nbytes) AS BIT VARYING));"
    done
    printf '%s\n' "$sql" | csql -u dba "$DB_NAME" >/dev/null 2>&1
}

setup_oos_table() {
    local table="$1"
    run_sql "DROP TABLE IF EXISTS $table;" >/dev/null 2>&1
    # BIT VARYING length is in BITS; 16,000,000 bits ~= 2MB, room for big rows.
    run_sql "CREATE TABLE $table (id INT PRIMARY KEY, big BIT VARYING(16000000));" >/dev/null 2>&1
}

# ============================================================================
# TC-01: DELETE -> vacuum interrupted by crash -> recovery
#
# Validates the REC_HOME (has_oos) and REC_RELOCATION sysop paths: a crash while
# vacuum reclaims deleted OOS-bearing records must leave NO orphan OOS records
# (leak) and NO dangling heap references (corruption).
# ============================================================================
test_delete_vacuum_crash() {
    log_msg "=== TC-01: DELETE + vacuum + crash + recovery ==="
    local table="t_vac_del"

    setup_oos_table "$table"
    insert_oos_rows "$table" 1 "$N_SMALL_ROWS" "AA" "$SMALL_BYTES"
    insert_oos_rows "$table" 1000 "$N_BIG_ROWS" "AA" "$BIG_BYTES"

    local total_rows=$((N_SMALL_ROWS + N_BIG_ROWS))
    assert_equals "TC-01 pre-crash row count" "$total_rows" "$(count_rows "$table")"

    local baseline
    baseline=$(oos_live_recs "$table")
    log_msg "TC-01 live OOS records after insert: $baseline"
    if [ "$baseline" = "-1" ] || [ "$baseline" -le 0 ]; then
        log_msg "FAIL: TC-01 expected OOS records to exist before delete (got '$baseline')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        return
    fi

    # Make every row dead, then crash while vacuum reclaims their OOS.
    run_sql "DELETE FROM $table;" >/dev/null 2>&1
    crash_during_vacuum "$table" "$baseline"

    # Recover.
    start_server_ready

    # Invariant 1: deleted rows stay deleted (DELETE was committed pre-crash).
    assert_equals "TC-01 post-recovery row count is 0" "0" "$(count_rows "$table")"

    # Invariant 2: structural integrity.
    run_checkdb

    # Invariant 3: no OOS leak — a post-recovery vacuum drains every chunk.
    if drain_oos "$table" "0"; then
        log_msg "PASS: TC-01 all OOS records reclaimed after recovery (no leak)"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        log_msg "FAIL: TC-01 OOS records NOT fully reclaimed (leak); live=$(oos_live_recs "$table")"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    # Invariant 4: table is fully reusable after recovery.
    insert_oos_rows "$table" 5000 1 "BB" "$SMALL_BYTES"
    assert_equals "TC-01 reused table reads new OOS value back" \
        "1" "$(value_match_count "$table" "big" "BB" "$SMALL_BYTES")"

    # Teardown is handled by the next fresh_db()/final cleanup_db() — no DROP
    # TABLE here (its internal temp volume is heavy and unnecessary).
    log_msg "=== TC-01 complete ==="
}

# ============================================================================
# TC-02: UPDATE -> vacuum interrupted by crash -> recovery
#
# The strongest check of the reviewer's concern. After UPDATE+commit the live
# record references the NEW OOS value (V2) while the dead pre-image references
# the OLD OOS value (V1). Vacuum's forward-walk must reclaim ONLY V1. A torn
# sysop would either free V1 while leaving a dangling pre-image, or worse free
# the live V2 -> the value-equality assertion below catches both.
# ============================================================================
test_update_vacuum_crash() {
    log_msg "=== TC-02: UPDATE + vacuum + crash + recovery (forward-walk) ==="
    local table="t_vac_upd"
    local v1_bytes=4096 v2_bytes=5120   # distinct sizes => distinct values

    setup_oos_table "$table"
    insert_oos_rows "$table" 1 "$N_SMALL_ROWS" "AA" "$v1_bytes"   # V1 = 'AA' x4096
    local n_rows="$N_SMALL_ROWS"
    assert_equals "TC-02 pre-update row count" "$n_rows" "$(count_rows "$table")"

    # Update every row to V2 = 'CC' x5120. Live -> V2 OOS; stale pre-image -> V1 OOS.
    run_sql "UPDATE $table SET big = CAST(REPEAT(X'CC', $v2_bytes) AS BIT VARYING);" >/dev/null 2>&1

    local baseline
    baseline=$(oos_live_recs "$table")   # ~ live V2 + stale V1
    log_msg "TC-02 live OOS records after update (V1 stale + V2 live): $baseline"

    crash_during_vacuum "$table" "$baseline"

    # Recover.
    start_server_ready

    # Invariant 1: all rows present and reading the NEW value EXACTLY.
    assert_equals "TC-02 post-recovery row count" "$n_rows" "$(count_rows "$table")"
    assert_equals "TC-02 every row reads V2 (no dangling/corruption)" \
        "$n_rows" "$(value_match_count "$table" "big" "CC" "$v2_bytes")"
    # Invariant 2: no row reverted to the old value V1.
    assert_equals "TC-02 no row reads stale V1" \
        "0" "$(value_match_count "$table" "big" "AA" "$v1_bytes")"

    # Invariant 3: structural integrity.
    run_checkdb

    # Invariant 4: stale V1 fully reclaimed, live V2 survives -> drains to n_rows.
    if drain_oos "$table" "$n_rows"; then
        log_msg "PASS: TC-02 stale V1 reclaimed, V2 survivors intact (live=$n_rows)"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        log_msg "FAIL: TC-02 OOS did not drain to $n_rows; live=$(oos_live_recs "$table")"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    # And the survivors must STILL read V2 after the final vacuum pass.
    assert_equals "TC-02 survivors still read V2 after final vacuum" \
        "$n_rows" "$(value_match_count "$table" "big" "CC" "$v2_bytes")"

    # Teardown handled by the final cleanup_db().
    log_msg "=== TC-02 complete ==="
}

# ============================================================================
# Main
# ============================================================================
# Fresh, healthy server for each scenario. A scenario ends with a kill -9 + a
# heavy DROP TABLE; rather than depend on that leaving a usable server behind,
# every scenario starts from a clean DB (mirrors oos_vacuum_crash.sh).
fresh_db() {
    cleanup_db
    create_db
    start_server_ready
}

main() {
    log_msg "======================================"
    log_msg "OOS Vacuum + Crash Recovery Test Suite"
    log_msg "======================================"

    fresh_db
    test_delete_vacuum_crash

    fresh_db
    test_update_vacuum_crash

    stop_server
    cleanup_db

    print_results
}

main "$@"

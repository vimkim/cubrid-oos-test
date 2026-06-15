#!/bin/bash
#
# OOS Vacuum Crash — "shell-max" hardening (CBRD-26668)
#
# Companion to oos_vacuum_crash_recovery.sh. That test crashes the server only
# AFTER vacuum has committed (often fully finished) the OOS reclaim, so it proves
# REDO + end-state consistency but NOT (a) that recovery did non-trivial work and
# NOT (b) the commit-BEFORE (mid-sysop) crash -> UNDO path. This script pushes
# both as far as a pure shell test can, without any engine change:
#
#   TC-A  REDO is non-trivial (deterministic)
#         checkpoint is DISABLED (checkpoint_every_size=10G) so committed vacuum
#         pages stay dirty in the buffer pool. One kill -9 after vacuum drains
#         the OOS -> on restart recovery MUST redo the whole chain from WAL to
#         reach the asserted end-state. We also grep the server log to confirm
#         the REDO phase actually ran. If recovery dropped/garbled the OOS
#         delete redo, the OOS would reappear (leak) -> FAIL.
#
#   TC-B  UNDO / mid-sysop crash (probabilistic fuzz)   [technique B + C]
#         Drop the "wait until oos_stats drops, then kill" gate. Instead: nudge
#         vacuum and kill -9 after a RANDOM 0..MAX_KILL_MS delay, looped N times,
#         so the crash lands at random points incl. INSIDE a sysop (before
#         commit). To make that window wide enough to actually hit, each round
#         feeds vacuum a FAT multi-chunk OOS value (~1MB => ~64 chunks): its
#         single vacuum sysop runs ~64 oos_delete calls, stretching the
#         pre-commit window from microseconds to milliseconds.
#         A set of KEEP rows (never deleted, their own OOS in the same file) must
#         read back EXACTLY at every crash point — the dangling/corruption
#         detector. After the loop, vacuum is driven to completion and the file
#         must drain to exactly the KEEP rows (no leak).
#
# Honest limit: TC-B is probabilistic — it samples the mid-sysop window, it does
# not guarantee hitting the single-instruction gap. Deterministic coverage of
# that gap still needs a debug-only fault-injection hook in vacuum_heap_record.
# See oos_vacuum_crash_recovery.md section 7.
#
# ---------------------------------------------------------------------------
# VALIDATION STATUS (2026-06-15, debug_gcc) & FUTURE WORK
# ---------------------------------------------------------------------------
#  * TC-A: VALIDATED. Recovery replayed 3435 WAL records, no OOS resurrected,
#    checkdb clean. Solid.
#  * TC-B mechanism: VALIDATED at scale — a full 20-iter run hit 10/20 crashes
#    during active reclaim with KEEP rows intact every time (real mid-reclaim
#    crashes, e.g. oos 16->8 across the crash). The undo-window sampling works.
#  * TC-B robustness fixes below are NOT yet re-validated end-to-end:
#      - auto_restart_server=no   (stop the double cub_server start race that
#        wedged recovery for ~2.5 min with "Latch promotion ... failed")
#      - TARGET_BACKLOG cap       (stop unbounded dead-OOS accumulation that
#        made each successive recovery heavier)
#      - readiness timeout 180s
#  * OPEN QUESTION: auto_restart_server is a PRM_FOR_CLIENT param; it is unproven
#    whether a per-DB [@dbname] section actually applies it to csql clients. If
#    a run still shows "not ready after 180s" + KEEP=-1, the [@db] override did
#    NOT take — set it in cubrid.conf [common] or via the client env instead,
#    and/or have kill_server kill ALL cub_server <db> instances.
#  * TODO: after the above is confirmed, run full defaults to green and record.
# ---------------------------------------------------------------------------
#
# Prerequisites: DEBUG build of CUBRID in PATH (;oos_stats metacommand).
# Usage: bash oos_vacuum_crash_fuzz.sh
#

# NB: CUBRID requires the DB-name log prefix to be < 17 chars.
DB_NAME="oosvacfuzz"
LOG_FILE="oos_vacuum_crash_fuzz_$(date +%Y%m%d_%H%M%S).log"

: "${DB_VOL_SIZE:=512M}"
: "${DB_LOG_SIZE:=256M}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

# Tunables (env-overridable)
: "${KEEP_ROWS:=8}"          # alive rows that must always read back exactly
: "${FUZZ_ITERS:=20}"        # number of random-timed crashes
: "${FAT_BYTES:=1048576}"    # ~1MB single value => multi-chunk fat sysop
: "${SMALL_BYTES:=4096}"     # small OOS rows / KEEP rows
: "${BACKLOG_FAT:=6}"        # initial fat dead rows (keep vacuum continuously busy)
: "${BACKLOG_SMALL:=20}"     # initial small dead rows
: "${TARGET_BACKLOG:=150}"   # cap pending dead-OOS chunks: only top up below this
                             # (unbounded accumulation makes crash recovery heavy)
: "${MAX_KILL_MS:=150}"      # random jitter AFTER vacuum starts, then kill: 0..this
: "${DRAIN_TIMEOUT:=180}"

CONF_FILE="$CUBRID/conf/cubrid.conf"
CONF_BEGIN="# OOS_FUZZ_CONF_BEGIN ${DB_NAME}"
CONF_END="# OOS_FUZZ_CONF_END ${DB_NAME}"

# ============================================================================
# Lifecycle (broker-free; csql connects directly to cub_server) + readiness gate
# ============================================================================

wait_for_server_ready() {
    local timeout="${1:-120}"
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

start_server() {
    log_msg "Starting server for $DB_NAME (no broker)..."
    cubrid server start "$DB_NAME" >>"$LOG_FILE" 2>&1
}
start_server_ready() { start_server; wait_for_server_ready 180; }
stop_server() { cubrid server stop "$DB_NAME" >>"$LOG_FILE" 2>&1; }
cleanup_db() {
    cubrid server stop "$DB_NAME" >/dev/null 2>&1
    cubrid deletedb "$DB_NAME" >/dev/null 2>&1
    rm -rf "$DB_VOL_PATH" >/dev/null 2>&1
    local db_txt="${CUBRID_DATABASES:-/tmp}/databases.txt"
    [ -f "$db_txt" ] && sed -i "/^${DB_NAME}[[:space:]]/d" "$db_txt"
    mkdir -p "$DB_VOL_PATH"
}

# Per-DB checkpoint setting via a [@DB_NAME] section in cubrid.conf. Applied at
# server start; cleaned up on exit. Only this DB is affected.
remove_db_conf() {
    [ -f "$CONF_FILE" ] && sed -i "/$CONF_BEGIN/,/$CONF_END/d" "$CONF_FILE"
}
write_db_conf() {   # $1 = checkpoint_every_size value (e.g. 10G or 16M)
    remove_db_conf
    {
        echo "$CONF_BEGIN"
        echo "[@${DB_NAME}]"
        echo "checkpoint_every_size=$1"
        # auto_restart_server defaults to yes: a client connecting to the
        # just-killed server makes the master spawn a SECOND instance while our
        # explicit 'cubrid server start' is already starting one. Two servers on
        # the same volumes deadlock recovery ("Latch promotion ... failed",
        # multi-minute boots). Force a single, explicit restart path.
        echo "auto_restart_server=no"
        echo "$CONF_END"
    } >> "$CONF_FILE"
    log_msg "cubrid.conf: [@${DB_NAME}] checkpoint_every_size=$1 auto_restart_server=no"
}

fresh_db_ckpt() {   # $1 = checkpoint_every_size; fresh DB with that setting
    cleanup_db
    write_db_conf "$1"
    create_db
    start_server_ready
}

# ============================================================================
# Query / vacuum helpers
# ============================================================================

scalar_query() {
    local out
    out=$(run_sql "$1")
    if echo "$out" | grep -qi "ERROR"; then echo "-1"; return; fi
    echo "$out" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]'
}
count_rows() { scalar_query "SELECT COUNT(*) FROM $1;"; }

# rows with id in [lo,hi] whose OOS column equals REPEAT(X'hex', nbytes)
value_match_count() {   # table lo hi hex nbytes
    scalar_query "SELECT COUNT(*) FROM $1 WHERE id BETWEEN $2 AND $3 \
                  AND big = CAST(REPEAT(X'$4', $5) AS BIT VARYING);"
}

oos_live_recs() {
    local out
    out=$(printf ';oos_stats %s\n' "$1" | csql -u dba "$DB_NAME" 2>&1)
    echo "$out" | grep -q "has no OOS file" && { echo "0"; return; }
    local n
    n=$(echo "$out" | sed -n 's/.*Live OOS records[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -1)
    [ -z "$n" ] && echo "-1" || echo "$n"
}

nudge_vacuum() { printf ';vacuum\n' | csql -u dba "$DB_NAME" >/dev/null 2>&1; }

gen_filler() {   # advance WAL so the delete's log block closes (vacuum eligible)
    local rounds="${1:-30}" i sql=""
    run_sql "CREATE TABLE IF NOT EXISTS t_filler (id INT PRIMARY KEY, pad BIT VARYING(40000));" >/dev/null 2>&1
    for ((i = 0; i < rounds; i++)); do
        sql+="INSERT INTO t_filler VALUES ($i, REPEAT(X'5A', 2048));DELETE FROM t_filler WHERE id=$i;"
    done
    printf '%s\n' "$sql" | csql -u dba "$DB_NAME" >/dev/null 2>&1
}

insert_rows() {   # table start_id count hex nbytes
    local table="$1" start="$2" cnt="$3" hex="$4" nb="$5" i sql=""
    for ((i = 0; i < cnt; i++)); do
        sql+="INSERT INTO $table VALUES ($((start + i)), CAST(REPEAT(X'$hex', $nb) AS BIT VARYING));"
    done
    printf '%s\n' "$sql" | csql -u dba "$DB_NAME" >/dev/null 2>&1
}

drain_oos() {   # table target timeout
    local table="$1" target="$2" timeout="${3:-$DRAIN_TIMEOUT}"
    local deadline=$(( $(date +%s) + timeout )) live
    while [ "$(date +%s)" -lt "$deadline" ]; do
        gen_filler 20; nudge_vacuum
        live=$(oos_live_recs "$table")
        [ "$live" = "$target" ] && return 0
        sleep 1
    done
    return 1
}

run_checkdb() {
    local out; out=$(cubrid checkdb "$DB_NAME" 2>&1)
    assert_not_contains "checkdb clean" "ERROR" "$out"
}

# Prove the restart did NON-TRIVIAL crash recovery. The post-crash server log
# prints e.g. "Log recovery: REDO Phase is started. ... Log records to redo: N".
# Under TC-A's disabled checkpoint the committed vacuum lives only in the WAL, so
# N must be > 0 (recovery genuinely replayed it). We read the NEWEST real server
# log by mtime (not the _latest.err symlink, whose target can be stale at the
# instant we check) and pull N out of the REDO-phase line.
assert_redo_nontrivial() {   # desc
    local newest n
    newest=$(ls -t "$CUBRID"/log/server/"${DB_NAME}"_2*.err 2>/dev/null | head -1)
    if [ -z "$newest" ] || [ ! -f "$newest" ]; then
        log_msg "FAIL: $1 — no server log found"; FAIL_COUNT=$((FAIL_COUNT + 1)); return
    fi
    n=$(grep -oE "Log records to redo: [0-9]+" "$newest" | grep -oE "[0-9]+" | tail -1)
    if [ -n "$n" ] && [ "$n" -gt 0 ]; then
        log_msg "PASS: $1 (REDO replayed $n log records; $(basename "$newest"))"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        log_msg "FAIL: $1 — no non-trivial REDO in $(basename "$newest")"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
}

setup_table() {
    run_sql "DROP TABLE IF EXISTS $1;" >/dev/null 2>&1
    run_sql "CREATE TABLE $1 (id INT PRIMARY KEY, big BIT VARYING(100000000));" >/dev/null 2>&1
}

# ============================================================================
# TC-A: REDO is non-trivial — checkpoint OFF + single post-vacuum crash
# ============================================================================
test_redo_nontrivial() {
    log_msg "=== TC-A: non-trivial REDO (checkpoint disabled) ==="
    local table="t_redo"
    setup_table "$table"

    insert_rows "$table" 1 "$KEEP_ROWS" "AA" "$SMALL_BYTES"     # OOS rows
    insert_rows "$table" 100 2 "BB" "$FAT_BYTES"                # fat multi-chunk
    local base; base=$(oos_live_recs "$table")
    log_msg "TC-A live OOS after insert: $base"
    assert_equals "TC-A rows present pre-crash" "$((KEEP_ROWS + 2))" "$(count_rows "$table")"

    run_sql "DELETE FROM $table;" >/dev/null 2>&1
    # Drive vacuum to FULLY reclaim, then crash. checkpoint is off, so the
    # reclaim lives only in WAL + dirty buffer; recovery must redo it.
    if ! drain_oos "$table" "0"; then
        log_msg "FAIL: TC-A vacuum did not drain before crash; live=$(oos_live_recs "$table")"
        FAIL_COUNT=$((FAIL_COUNT + 1)); return
    fi
    log_msg "TC-A vacuum drained to 0 (committed, not yet flushed); crashing..."
    kill_server
    start_server_ready

    assert_redo_nontrivial "TC-A REDO replayed committed vacuum from WAL"
    assert_equals "TC-A post-recovery rows = 0" "0" "$(count_rows "$table")"
    # If the OOS-delete redo were dropped, freed OOS would 'reappear' as live.
    local after; after=$(oos_live_recs "$table")
    assert_equals "TC-A no OOS resurrected/leaked after REDO" "0" "$after"
    run_checkdb
    log_msg "=== TC-A complete ==="
}

# ============================================================================
# TC-B: UNDO / mid-sysop crash — random-timed kill fuzz over a fat sysop
# ============================================================================
# Poll until vacuum actually STARTS reclaiming (live count drops below baseline)
# or a timeout. Returns 0 if it started.
#
# Crucially this KEEPS generating filler + nudging each round: CUBRID's vacuum
# master refuses a log block until the append head is >=2 blocks past it, so a
# single gen_filler before the loop is not enough — without continuous filler
# the block never becomes eligible and vacuum never starts (observed: live OOS
# grows, nothing reclaimed). This mirrors drain_oos but returns the instant the
# count first drops, so the subsequent kill lands while vacuum is still busy.
wait_vacuum_started() {   # table baseline timeout_sec
    local table="$1" baseline="$2" deadline=$(( $(date +%s) + ${3:-10} )) now
    while [ "$(date +%s)" -lt "$deadline" ]; do
        gen_filler 15
        nudge_vacuum
        now=$(oos_live_recs "$table")
        [ "$now" != "-1" ] && [ "$now" -lt "$baseline" ] && return 0
    done
    return 1
}

test_undo_fuzz() {
    log_msg "=== TC-B: mid-sysop crash fuzz (${FUZZ_ITERS} iters, fat=${FAT_BYTES}B) ==="
    local table="t_fuzz"
    setup_table "$table"

    # KEEP rows: alive forever, must read back EXACTLY at every crash point.
    insert_rows "$table" 1 "$KEEP_ROWS" "EE" "$SMALL_BYTES"
    assert_equals "TC-B KEEP rows created" "$KEEP_ROWS" \
        "$(value_match_count "$table" 1 "$KEEP_ROWS" "EE" "$SMALL_BYTES")"

    # Big dead backlog up front so vacuum has CONTINUOUS work — a kill during the
    # active phase then reliably lands while some sysop is still in flight
    # (uncommitted), which is exactly the UNDO window the other test can't reach.
    insert_rows "$table" 1000 "$BACKLOG_FAT" "AA" "$FAT_BYTES"
    insert_rows "$table" 1100 "$BACKLOG_SMALL" "CC" "$SMALL_BYTES"
    run_sql "DELETE FROM $table WHERE id >= 1000;" >/dev/null 2>&1
    gen_filler 40

    local i progressed=0 nextid=2000
    for ((i = 1; i <= FUZZ_ITERS; i++)); do
        # Top up the backlog ONLY when it has drained below target. When a crash
        # interrupts vacuum the dead OOS lingers, so unconditional inserts would
        # accumulate without bound and make each successive recovery heavier.
        # This self-regulates: full backlog -> add nothing; drained -> refill.
        local pending; pending=$(oos_live_recs "$table")
        pending=$(( ${pending:-0} - KEEP_ROWS ))
        if [ "$pending" -lt "$TARGET_BACKLOG" ]; then
            insert_rows "$table" "$nextid" 2 "AA" "$FAT_BYTES"
            insert_rows "$table" "$((nextid + 2))" 4 "CC" "$SMALL_BYTES"
            nextid=$((nextid + 10))
            run_sql "DELETE FROM $table WHERE id >= 1000;" >/dev/null 2>&1
            gen_filler 20
        fi

        local before; before=$(oos_live_recs "$table")
        nudge_vacuum
        local started=0
        wait_vacuum_started "$table" "$before" 5 && started=1

        # random jitter INTO the active window, then crash mid-stream
        local ms=$(( RANDOM % (MAX_KILL_MS + 1) ))
        sleep "$(printf '%d.%03d' $((ms / 1000)) $((ms % 1000)))"
        kill_server
        start_server_ready

        local after; after=$(oos_live_recs "$table")
        [ "$started" = "1" ] && [ "$after" != "-1" ] && [ "$after" -lt "$before" ] && progressed=$((progressed + 1))

        # Invariant that must hold at EVERY crash point:
        local keep; keep=$(value_match_count "$table" 1 "$KEEP_ROWS" "EE" "$SMALL_BYTES")
        if [ "$keep" = "$KEEP_ROWS" ]; then
            PASS_COUNT=$((PASS_COUNT + 1))
        else
            log_msg "FAIL: iter $i — KEEP corrupted/dangling (got $keep/$KEEP_ROWS; started=$started kill@${ms}ms)"
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
        log_msg "iter $i: started=$started kill@${ms}ms oos $before->$after KEEP=$keep/$KEEP_ROWS"
    done

    # The fuzz only means something if crashes actually hit active vacuum.
    log_msg "TC-B: $progressed/$FUZZ_ITERS crashes landed during active reclaim"
    if [ "$progressed" -ge 1 ]; then
        log_msg "PASS: TC-B sampled the active-vacuum (mid-sysop) window"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        log_msg "FAIL: TC-B never crashed during active reclaim — fuzz ineffective (raise BACKLOG_FAT/iters)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    run_checkdb
    if drain_oos "$table" "$KEEP_ROWS"; then
        log_msg "PASS: TC-B drained to KEEP rows only (no leak after the crash storm)"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        log_msg "FAIL: TC-B did not drain to $KEEP_ROWS; live=$(oos_live_recs "$table")"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    assert_equals "TC-B KEEP rows intact after full vacuum" "$KEEP_ROWS" \
        "$(value_match_count "$table" 1 "$KEEP_ROWS" "EE" "$SMALL_BYTES")"
    log_msg "=== TC-B complete ==="
}

# ============================================================================
# Main
# ============================================================================
main() {
    log_msg "======================================"
    log_msg "OOS Vacuum Crash — shell-max hardening"
    log_msg "======================================"

    trap 'remove_db_conf' EXIT

    fresh_db_ckpt "10G"     # checkpoint effectively OFF for the REDO proof
    test_redo_nontrivial

    fresh_db_ckpt "16M"     # frequent checkpoints keep the fuzz loop fast
    test_undo_fuzz

    stop_server
    remove_db_conf
    cleanup_db

    print_results
}

main "$@"

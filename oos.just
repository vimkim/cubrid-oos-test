set shell := ["bash", "-cu"]

db := env('OOS_TEST_DB', 'testdb')
# directory where this justfile lives (the test repo)
test_dir := source_directory()

# run all OOS tests (sql + shell)
all: sql shell

# ──────────────────────────────────────────────
# SQL tests
# ──────────────────────────────────────────────

# run all SQL tests and report pass/fail (requires: cubrid server start + cubrid broker start)
sql:
    #!/usr/bin/env bash
    set -u
    PASS=0; FAIL=0; TOTAL=0
    for f in "{{test_dir}}"/sql/*.sql; do
        TOTAL=$((TOTAL + 1))
        name=$(basename "$f")
        echo "=== [$TOTAL] $name ==="
        output=$(csql -u dba "{{db}}" -i "$f" 2>&1)
        rc=$?
        if [ $rc -ne 0 ] || echo "$output" | grep -qi "ERROR"; then
            echo "FAIL: $name (exit=$rc)"
            echo "$output" | grep -i "ERROR" | head -5
            FAIL=$((FAIL + 1))
        else
            echo "PASS: $name"
            PASS=$((PASS + 1))
        fi
        echo ""
    done
    echo "=============================="
    echo "SQL Results: PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL"
    echo "=============================="
    [ "$FAIL" -eq 0 ]

# run a single SQL test file by name (e.g. just oos sql-one oos_basic_crud)
sql-one name:
    csql -u dba "{{db}}" -i "{{test_dir}}/sql/{{name}}.sql"

# run SQL tests with output capture for diff-based validation
sql-golden:
    #!/usr/bin/env bash
    set -u
    mkdir -p "{{test_dir}}/sql/answer"
    PASS=0; FAIL=0
    for f in "{{test_dir}}"/sql/*.sql; do
        name=$(basename "$f" .sql)
        answer="{{test_dir}}/sql/answer/${name}.answer"
        actual=$(csql -u dba "{{db}}" -i "$f" 2>&1)
        if [ -f "$answer" ]; then
            if diff <(echo "$actual") "$answer" > /dev/null 2>&1; then
                echo "PASS: $name (matches golden)"
                PASS=$((PASS + 1))
            else
                echo "FAIL: $name (output differs from golden)"
                diff <(echo "$actual") "$answer" | head -20
                FAIL=$((FAIL + 1))
            fi
        else
            echo "SKIP: $name (no golden file, generating...)"
            echo "$actual" > "$answer"
        fi
    done
    echo "=============================="
    echo "Golden Results: PASS=$PASS FAIL=$FAIL"
    echo "=============================="
    [ "$FAIL" -eq 0 ]

# ──────────────────────────────────────────────
# Shell tests
# ──────────────────────────────────────────────

# run all shell tests (crash recovery, MVCC, stress) and report pass/fail
shell:
    #!/usr/bin/env bash
    set -u
    PASS=0; FAIL=0; TOTAL=0
    for f in "{{test_dir}}"/shell/*.sh; do
        name=$(basename "$f")
        # skip replication test by default (requires HA setup)
        if [ "$name" = "oos_replication.sh" ]; then
            echo "=== SKIP: $name (requires HA setup, use 'just oos shell-repl') ==="
            echo ""
            continue
        fi
        TOTAL=$((TOTAL + 1))
        echo "=== [$TOTAL] $name ==="
        bash "$f" "{{db}}" 2>&1
        rc=$?
        if [ $rc -ne 0 ]; then
            echo "FAIL: $name (exit=$rc)"
            FAIL=$((FAIL + 1))
        else
            echo "PASS: $name"
            PASS=$((PASS + 1))
        fi
        echo ""
    done
    echo "=============================="
    echo "Shell Results: PASS=$PASS FAIL=$FAIL TOTAL=$TOTAL"
    echo "=============================="
    [ "$FAIL" -eq 0 ]

# run a single shell test by name (e.g. just oos shell-one oos_crash_recovery)
shell-one name:
    bash "{{test_dir}}/shell/{{name}}.sh" "{{db}}"

# run crash recovery tests only
shell-crash:
    bash "{{test_dir}}/shell/oos_crash_recovery.sh"

# run MVCC isolation tests only
shell-mvcc:
    bash "{{test_dir}}/shell/oos_mvcc_isolation.sh" "{{db}}"

# run stress/bulk tests only
shell-stress:
    bash "{{test_dir}}/shell/oos_stress_bulk.sh"

# run replication tests (requires HA master/slave)
shell-repl master='localhost' slave='localhost':
    bash "{{test_dir}}/shell/oos_replication.sh" "{{master}}" "{{slave}}"

# ──────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────

# list all test files
list:
    #!/usr/bin/env bash
    echo "SQL tests:"
    ls -1 "{{test_dir}}"/sql/*.sql
    echo ""
    echo "Shell tests:"
    ls -1 "{{test_dir}}"/shell/*.sh

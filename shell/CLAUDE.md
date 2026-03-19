# Shell Tests

## Structure

- `common.sh` — shared helpers sourced by all test scripts (inspired by CTP `init_path/init.sh`)
- `oos_*.sh` — individual test scripts

## Writing a New Test Script

```bash
#!/bin/bash
DB_NAME="oos_my_test"
LOG_FILE="oos_my_test_$(date +%Y%m%d_%H%M%S).log"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# ... test functions using helpers from common.sh ...

main() {
    log_msg "======================================"
    log_msg "My Test Suite"
    log_msg "======================================"

    cleanup_db
    create_db
    start_server

    # call test functions here

    stop_server
    cleanup_db

    print_results
}

main "$@"
```

## common.sh API

| Function | Description |
|---|---|
| `log_msg MSG...` | Timestamped log to stdout + `$LOG_FILE` |
| `run_sql "SQL"` | Execute SQL via `csql -u dba $DB_NAME` |
| `run_sql_file FILE` | Execute SQL file via `csql -u dba $DB_NAME -i` |
| `assert_contains DESC SUBSTR ACTUAL` | PASS if `$ACTUAL` contains `$SUBSTR` |
| `assert_not_contains DESC SUBSTR ACTUAL` | PASS if `$ACTUAL` does NOT contain `$SUBSTR` |
| `assert_equals DESC EXPECTED ACTUAL` | PASS if `$EXPECTED` = `$ACTUAL` |
| `create_db` | `cubrid createdb` with `$DB_VOL_SIZE` / `$DB_LOG_SIZE` |
| `start_server` | Start cub_server + broker for `$DB_NAME` |
| `stop_server` | Graceful stop broker + server |
| `kill_server` | `kill -9` the cub_server (crash simulation) |
| `cleanup_db` | Stop, deletedb, remove volume dir |
| `print_results` | Print PASS/FAIL counts and `exit 1` on failures |

## Required Variables (set before `source common.sh`)

| Variable | Required | Description |
|---|---|---|
| `DB_NAME` | yes | Database name |
| `LOG_FILE` | yes | Log file path |
| `DB_VOL_SIZE` | no | createdb volume size (default: `512M`) |
| `DB_LOG_SIZE` | no | createdb log volume size (default: `256M`) |
| `DB_VOL_PATH` | no | Volume directory (default: `${CUBRID_DATABASES:-/tmp}/$DB_NAME`) |

## Conventions

- Tests that manage their own DB (crash_recovery, stress_bulk): call `cleanup_db`/`create_db`/`start_server` in main, `stop_server`/`cleanup_db` at the end.
- Tests that expect an existing DB (mvcc_isolation): accept `$DB_NAME` as `$1`, skip DB lifecycle.
- Replication tests: define their own `run_sql_master()`/`run_sql_slave()` on top of common.sh.
- All main functions end with `print_results` (not duplicated exit logic).

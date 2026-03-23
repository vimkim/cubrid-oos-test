# cubrid-oos-test

External test suite for CUBRID **OOS (Out-of-Slot)** storage feature.

OOS allows CUBRID to store large variable-length column values outside the fixed-size heap slot, avoiding slot overflow for wide rows.

## Quick Start

This repo is used as a **justfile module**. From any CUBRID worktree that imports it:

```bash
just oos list          # list all test files
just oos sql           # run all SQL tests
just oos shell         # run all shell tests
just oos all           # run everything
```

### Worktree Integration

Add this line to your worktree's `justfile`:

```just
mod? oos '/home/vimkim/gh/cubrid-oos-test/oos.just'
```

## Commands

| Command | Description |
|---------|-------------|
| `just oos list` | List all test files |
| `just oos sql` | Run all SQL tests (needs `cubrid server start` + `cubrid broker start`) |
| `just oos shell` | Run all shell tests (skips replication by default) |
| `just oos all` | Run SQL + shell tests |
| `just oos sql-one NAME` | Run single SQL test (e.g. `oos_basic_crud`) |
| `just oos sql-golden` | Diff-based validation against golden answer files |
| `just oos shell-crash` | Crash recovery tests only |
| `just oos shell-mvcc` | MVCC isolation tests only |
| `just oos shell-stress` | Stress/bulk tests only |
| `just oos shell-repl` | Replication tests (requires HA setup) |

## Structure

```
oos.just              # justfile module entry point
lib/common.sh         # shared shell helpers
sql/                  # SQL tests (run via csql)
  ├── oos_basic_crud.sql
  ├── oos_boundary_edge.sql
  ├── oos_ddl_operations.sql
  ├── oos_delete_api.sql
  ├── oos_transaction_mvcc.sql
  ├── oos_update_delete.sql
  └── answer/         # golden files for sql-golden
shell/                # shell tests (manage their own databases)
  ├── oos_crash_recovery.sh
  ├── oos_delete_physical.sh
  ├── oos_mvcc_isolation.sh
  ├── oos_replication.sh
  └── oos_stress_bulk.sh
```

## Configuration

- `OOS_TEST_DB` env var sets the database name (default: `testdb`)
- SQL tests require a running CUBRID server and broker
- Shell tests create/destroy their own databases

## CBRD Coverage

| CBRD | Area |
|------|------|
| CBRD-26352 | OOS INSERT/SELECT, data_readval COPY |
| CBRD-26358 | OOS boundary conditions |
| CBRD-26458 | unloaddb/loaddb with OOS |
| CBRD-26463 | OOS logging/recovery |
| CBRD-26488 | MVCC header size lookup |
| CBRD-26521 | OOS UPDATE/DELETE, replication |
| CBRD-26547 | Covered index (midxkey) |
| CBRD-26565 | midxkey buffer overflow |
| CBRD-26608 | DROP TABLE OOS cleanup |

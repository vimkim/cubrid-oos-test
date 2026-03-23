# cubrid-oos-test

External test suite for CUBRID OOS (Out-of-Slot) storage feature.
Designed to run from any CUBRID OOS worktree via `just oos <command>`.

## Structure

```
├── oos.just          # justfile module (imported by worktree justfiles)
├── lib/
│   └── common.sh     # shared helpers sourced by shell tests
├── sql/              # SQL-based tests (run via csql)
│   ├── oos_basic_crud.sql
│   ├── oos_boundary_edge.sql
│   ├── oos_ddl_operations.sql
│   ├── oos_transaction_mvcc.sql
│   └── oos_update_delete.sql
└── shell/            # Shell-based tests (multi-session, crash recovery)
    ├── oos_crash_recovery.sh
    ├── oos_mvcc_isolation.sh
    ├── oos_replication.sh
    └── oos_stress_bulk.sh
```

## Usage

From any CUBRID worktree that imports this module:

```bash
just oos list          # list all test files
just oos sql           # run all SQL tests
just oos shell         # run all shell tests (skips replication by default)
just oos all           # run everything
just oos sql-one NAME  # run single SQL test (e.g. oos_basic_crud)
just oos shell-crash   # crash recovery tests only
just oos shell-mvcc    # MVCC isolation tests only
just oos shell-stress  # stress/bulk tests only
just oos shell-repl    # replication tests (requires HA setup)
just oos sql-golden    # diff-based validation with golden files
```

## Configuration

- `OOS_TEST_DB` env var: database name (default: `testdb`)
- SQL tests require: `cubrid server start` + `cubrid broker start`
- Shell tests create/destroy their own databases

## Worktree Integration

Each worktree's `justfile` needs this line:

```just
mod? oos '/home/vimkim/gh/cubrid-oos-test/oos.just'
```

This is handled automatically via stow (`~/my-cubrid/stow/cubrid/justfile`).

## CBRD Coverage

| CBRD | Area | Tests |
|------|------|-------|
| CBRD-26352 | OOS INSERT/SELECT, data_readval COPY | basic_crud, boundary_edge, transaction_mvcc |
| CBRD-26358 | OOS boundary conditions | basic_crud |
| CBRD-26458 | unloaddb/loaddb with OOS | stress_bulk |
| CBRD-26463 | OOS logging/recovery | crash_recovery, mvcc_isolation |
| CBRD-26488 | MVCC header size lookup | boundary_edge |
| CBRD-26521 | OOS UPDATE/DELETE, replication | update_delete, replication |
| CBRD-26547 | Covered index (midxkey) | boundary_edge |
| CBRD-26565 | midxkey buffer overflow | boundary_edge |
| CBRD-26608 | DROP TABLE OOS cleanup | boundary_edge, crash_recovery, ddl_operations |

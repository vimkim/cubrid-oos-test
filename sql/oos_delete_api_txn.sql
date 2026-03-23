--
-- OOS Delete API Transaction Tests (requires --no-auto-commit)
--
-- CBRD-26609: implement OOS delete API
-- Tests: DELETE inside committed transaction, DELETE inside rolled-back transaction
--

-- ============================================================================
-- TC-01: DELETE inside committed transaction
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_commit;

CREATE TABLE t_oos_del_commit (
    id INT PRIMARY KEY,
    data_col BIT VARYING
);

INSERT INTO t_oos_del_commit VALUES (1, REPEAT(X'AA', 2048));
COMMIT;

DELETE FROM t_oos_del_commit WHERE id = 1;
COMMIT;

SELECT COUNT(*) AS cnt FROM t_oos_del_commit;

DROP TABLE t_oos_del_commit;

-- ============================================================================
-- TC-02: DELETE inside rolled-back transaction
-- After ROLLBACK, the deleted OOS row should still be visible.
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_rollback;

CREATE TABLE t_oos_del_rollback (
    id INT PRIMARY KEY,
    data_col BIT VARYING
);

INSERT INTO t_oos_del_rollback VALUES (1, REPEAT(X'AA', 2048));
COMMIT;

DELETE FROM t_oos_del_rollback WHERE id = 1;
ROLLBACK;

-- Row should still exist after rollback
SELECT id, LENGTH(data_col) AS len FROM t_oos_del_rollback WHERE id = 1;

DROP TABLE t_oos_del_rollback;

--
-- OOS Delete API Tests
--
-- CBRD-26609: implement OOS delete API
-- Tests: DELETE single-chunk OOS, DELETE multi-chunk OOS, DELETE + reinsert,
--        DELETE multiple rows, DELETE with mixed OOS/non-OOS columns,
--        DELETE in transaction (commit/rollback)
--

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS t_oos_delete_api;

-- ============================================================================
-- TC-01: DELETE a single-chunk OOS record
-- Insert a row with OOS column (~1KB), delete it, verify it's gone.
-- ============================================================================

CREATE TABLE t_oos_delete_api (
    id INT PRIMARY KEY,
    label VARCHAR(50),
    data_col BIT VARYING
);

INSERT INTO t_oos_delete_api VALUES (1, 'single_chunk', REPEAT(X'AA', 1024));

SELECT id, label, LENGTH(data_col) AS len FROM t_oos_delete_api WHERE id = 1;

DELETE FROM t_oos_delete_api WHERE id = 1;

SELECT COUNT(*) AS cnt FROM t_oos_delete_api WHERE id = 1;

DROP TABLE t_oos_delete_api;

-- ============================================================================
-- TC-02: DELETE a multi-chunk OOS record (32KB, spans multiple OOS pages)
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_multi;

CREATE TABLE t_oos_del_multi (
    id INT PRIMARY KEY,
    huge_col BIT VARYING
);

INSERT INTO t_oos_del_multi VALUES (1, REPEAT(X'BB', 32768));

SELECT id, LENGTH(huge_col) AS len FROM t_oos_del_multi WHERE id = 1;

DELETE FROM t_oos_del_multi WHERE id = 1;

SELECT COUNT(*) AS cnt FROM t_oos_del_multi WHERE id = 1;

DROP TABLE t_oos_del_multi;

-- ============================================================================
-- TC-03: DELETE a very large multi-chunk OOS record (160KB)
-- Matches the unit test OosDeleteLarge160KBMultiChunk scenario.
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_160k;

CREATE TABLE t_oos_del_160k (
    id INT PRIMARY KEY,
    big_col BIT VARYING
);

INSERT INTO t_oos_del_160k VALUES (1, REPEAT(X'CC', 163840));

SELECT id, LENGTH(big_col) AS len FROM t_oos_del_160k WHERE id = 1;

DELETE FROM t_oos_del_160k WHERE id = 1;

SELECT COUNT(*) AS cnt FROM t_oos_del_160k;

DROP TABLE t_oos_del_160k;

-- ============================================================================
-- TC-04: DELETE one row among multiple OOS rows (other rows unaffected)
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_selective;

CREATE TABLE t_oos_del_selective (
    id INT PRIMARY KEY,
    data_col BIT VARYING
);

INSERT INTO t_oos_del_selective VALUES (1, REPEAT(X'AA', 1024));
INSERT INTO t_oos_del_selective VALUES (2, REPEAT(X'BB', 2048));
INSERT INTO t_oos_del_selective VALUES (3, REPEAT(X'CC', 4096));

DELETE FROM t_oos_del_selective WHERE id = 2;

-- Remaining rows must be intact
SELECT id, LENGTH(data_col) AS len,
       SUBSTRING(data_col FROM 1 FOR 2) AS prefix
FROM t_oos_del_selective ORDER BY id;

DROP TABLE t_oos_del_selective;

-- ============================================================================
-- TC-05: DELETE then reinsert into same table
-- After deleting OOS records, new inserts should work correctly.
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_reinsert;

CREATE TABLE t_oos_del_reinsert (
    id INT PRIMARY KEY,
    data_col BIT VARYING
);

INSERT INTO t_oos_del_reinsert VALUES (1, REPEAT(X'AA', 2048));
INSERT INTO t_oos_del_reinsert VALUES (2, REPEAT(X'BB', 4096));

DELETE FROM t_oos_del_reinsert;

SELECT COUNT(*) AS cnt_after_delete FROM t_oos_del_reinsert;

-- Reinsert
INSERT INTO t_oos_del_reinsert VALUES (10, REPEAT(X'DD', 2048));
INSERT INTO t_oos_del_reinsert VALUES (20, REPEAT(X'EE', 8192));

SELECT id, LENGTH(data_col) AS len,
       SUBSTRING(data_col FROM 1 FOR 2) AS prefix
FROM t_oos_del_reinsert ORDER BY id;

DROP TABLE t_oos_del_reinsert;

-- ============================================================================
-- TC-06: DELETE row with mixed OOS and non-OOS columns
-- Verify that both OOS and in-heap data are properly cleaned up.
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_mixed;

CREATE TABLE t_oos_del_mixed (
    id INT PRIMARY KEY,
    small_col VARCHAR(100),
    oos_col1 BIT VARYING,
    oos_col2 BIT VARYING
);

INSERT INTO t_oos_del_mixed VALUES (1, 'keep_me', REPEAT(X'AA', 1024), REPEAT(X'BB', 2048));
INSERT INTO t_oos_del_mixed VALUES (2, 'delete_me', REPEAT(X'CC', 4096), REPEAT(X'DD', 8192));

DELETE FROM t_oos_del_mixed WHERE id = 2;

-- Row 1 must be unaffected
SELECT id, small_col, LENGTH(oos_col1) AS len1, LENGTH(oos_col2) AS len2
FROM t_oos_del_mixed ORDER BY id;

SELECT COUNT(*) AS total FROM t_oos_del_mixed;

DROP TABLE t_oos_del_mixed;

-- ============================================================================
-- TC-07: UPDATE OOS column (simulates insert-new + delete-old pattern)
-- Under the hood, UPDATE on OOS column inserts a new OOS record and
-- the old one is deleted (by vacuum or directly via oos_delete).
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_update;

CREATE TABLE t_oos_del_update (
    id INT PRIMARY KEY,
    data_col BIT VARYING
);

INSERT INTO t_oos_del_update VALUES (1, REPEAT(X'AA', 2048));

SELECT id, LENGTH(data_col) AS len,
       SUBSTRING(data_col FROM 1 FOR 2) AS prefix
FROM t_oos_del_update WHERE id = 1;

-- UPDATE replaces OOS value; old OOS record should be deleted
UPDATE t_oos_del_update SET data_col = REPEAT(X'FF', 4096) WHERE id = 1;

SELECT id, LENGTH(data_col) AS len,
       SUBSTRING(data_col FROM 1 FOR 2) AS prefix
FROM t_oos_del_update WHERE id = 1;

DROP TABLE t_oos_del_update;

-- ============================================================================
-- TC-08: Bulk DELETE of many OOS records (was TC-10)
-- Insert 50 OOS records, delete all, verify table is empty.
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_bulk;

CREATE TABLE t_oos_del_bulk (
    id INT PRIMARY KEY AUTO_INCREMENT,
    data_col BIT VARYING
);

INSERT INTO t_oos_del_bulk (data_col)
    SELECT REPEAT(X'EE', 1024 + (ROWNUM * 20))
    FROM db_class a, db_class b
    LIMIT 50;

SELECT COUNT(*) AS before_delete FROM t_oos_del_bulk;

DELETE FROM t_oos_del_bulk;

SELECT COUNT(*) AS after_delete FROM t_oos_del_bulk;

DROP TABLE t_oos_del_bulk;

-- ============================================================================
-- TC-09: DELETE with multi-chunk then reinsert multi-chunk (was TC-11)
-- Ensures page space is reclaimed and reusable after chain deletion.
-- ============================================================================

DROP TABLE IF EXISTS t_oos_del_reuse;

CREATE TABLE t_oos_del_reuse (
    id INT PRIMARY KEY,
    big_col BIT VARYING
);

-- Insert 64KB multi-chunk record
INSERT INTO t_oos_del_reuse VALUES (1, REPEAT(X'AA', 65536));

SELECT id, LENGTH(big_col) AS len FROM t_oos_del_reuse;

DELETE FROM t_oos_del_reuse WHERE id = 1;

-- Reinsert another large record (should reuse freed pages)
INSERT INTO t_oos_del_reuse VALUES (2, REPEAT(X'BB', 65536));

SELECT id, LENGTH(big_col) AS len,
       SUBSTRING(big_col FROM 1 FOR 2) AS prefix
FROM t_oos_del_reuse;

DROP TABLE t_oos_del_reuse;

CREATE SCHEMA data_engineering;

CREATE TABLE tables_usage(
    dbname varchar(16) ENCODE ZSTD NOT NULL, 
    schemaname varchar(128) ENCODE ZSTD NOT NULL, 
    table_id INT4 ENCODE ZSTD NOT NULL, 
    tablename varchar(128) ENCODE ZSTD NOT NULL,
    size INT8 ENCODE ZSTD NOT NULL, 
    last_used date NOT NULL
)
DISTKEY (table_id)
SORTKEY (last_used);

DELETE FROM data_engineering.tables_usage
WHERE table_id NOT IN (
	SELECT table_id FROM svv_table_info
);

INSERT INTO data_engineering.tables_usage
SELECT
    t.database AS dbname,
    t.schema AS schemaname,
    t.table_id,
    t."table" AS tablename,
    t.size,
    '1900-01-01 00:00:00' as last_used --arbitrary date
FROM
	svv_table_info t --we take every table
LEFT JOIN data_engineering.tables_usage tu USING (table_id)
WHERE
	t."schema" NOT IN ('pg_internal','information_schema','pg_catalog')
	AND t."schema" NOT LIKE 'pg_temp_%'
	AND tu.table_id IS NULL;


UPDATE data_engineering.tables_usage SET last_used=s.last_scan
FROM (
    SELECT
        s.tbl::INT,
        trunc(max(starttime)) as last_scan
    FROM
        stl_scan s
    WHERE s.userid NOT IN (1) -- we remove user rdsdb
    AND s.perm_table_name NOT IN ('Internal Worktable','S3')
    GROUP BY s.tbl
) s
WHERE (s.tbl = table_id and last_used<s.last_scan);

-- PostgreSQL Edge Case Test Script
-- Each unit is designed to be a semantic unit that could trip up the parser

-- Unit 1: Database and extensions setup
CREATE DATABASE IF NOT EXISTS edge_case_db
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Unit 2: Extension installations
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "hstore";
CREATE EXTENSION IF NOT EXISTS "ltree";
CREATE EXTENSION IF NOT EXISTS "cube";
CREATE EXTENSION IF NOT EXISTS "earthdistance";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "btree_gist";
CREATE EXTENSION IF NOT EXISTS "postgres_fdw";
CREATE EXTENSION IF NOT EXISTS "file_fdw";
CREATE EXTENSION IF NOT EXISTS "dblink";
CREATE EXTENSION IF NOT EXISTS "tablefunc";
CREATE EXTENSION IF NOT EXISTS "unaccent";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Unit 3: Custom types and domains
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', 'excited', 'angry');
CREATE TYPE complex_number AS (
    r double precision,
    i double precision
);
CREATE TYPE inventory_item AS (
    name text,
    supplier_id integer,
    price numeric(10,2)
);

-- Unit 4: Domain constraints
CREATE DOMAIN us_postal_code AS text
    CHECK (VALUE ~ '^\d{5}$' OR VALUE ~ '^\d{5}-\d{4}$');

CREATE DOMAIN email_address AS varchar(255)
    CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    NOT NULL;

CREATE DOMAIN positive_integer AS integer
    CHECK (VALUE > 0);

CREATE DOMAIN percentage AS numeric(5,2)
    CHECK (VALUE >= 0 AND VALUE <= 100);

-- Unit 5: Schema creation with authorization
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS archive;

-- Unit 6: Complex table with all column types
CREATE TABLE IF NOT EXISTS public.everything_table (
    -- Numeric types
    id bigserial PRIMARY KEY,
    small_int smallint,
    normal_int integer DEFAULT 42,
    big_int bigint NOT NULL,
    decimal_num decimal(19,4),
    numeric_num numeric(1000,500),
    real_num real,
    double_num double precision,
    small_serial smallserial,
    normal_serial serial,
    big_serial bigserial,

    -- Monetary type
    price money,

    -- Character types
    char_fixed char(10),
    varchar_limited varchar(255) COLLATE "C",
    text_unlimited text,

    -- Binary data
    byte_data bytea,

    -- Date/Time types
    date_only date DEFAULT CURRENT_DATE,
    time_only time,
    time_with_zone time with time zone,
    timestamp_no_zone timestamp,
    timestamp_with_zone timestamptz DEFAULT NOW(),
    interval_time interval,

    -- Boolean
    is_active boolean DEFAULT true,

    -- Geometric types
    point_2d point,
    line_segment lseg,
    box_area box,
    path_open path,
    polygon_closed polygon,
    circle_shape circle,

    -- Network types
    ip_address inet,
    network_cidr cidr,
    mac_address macaddr,
    mac_address8 macaddr8,

    -- Bit strings
    bit_fixed bit(8),
    bit_varying bit varying(64),

    -- UUID
    unique_id uuid DEFAULT uuid_generate_v4(),

    -- XML and JSON
    xml_data xml,
    json_data json,
    jsonb_data jsonb DEFAULT '{}'::jsonb,

    -- Arrays
    integer_array integer[],
    text_array text[][] NOT NULL DEFAULT '{}',
    multidim_array integer[3][3],

    -- Custom types
    current_mood mood,
    complex_num complex_number,
    inventory inventory_item,

    -- Range types
    int_range int4range,
    bigint_range int8range,
    numeric_range numrange,
    timestamp_range tsrange,
    timestamptz_range tstzrange,
    date_range daterange,

    -- Full-text search
    search_vector tsvector,
    search_query tsquery,

    -- Other PostgreSQL types
    object_id oid,
    row_id tid,
    transaction_id xid,
    command_id cid,

    -- Special types
    hstore_data hstore,
    ltree_path ltree,
    cube_data cube,

    -- Constraints at column level
    email email_address,
    postal_code us_postal_code,
    positive_num positive_integer,
    percentage_val percentage,

    -- Check constraints
    CONSTRAINT check_positive CHECK (normal_int > 0),
    CONSTRAINT check_percentage CHECK (percentage_val BETWEEN 0 AND 100),

    -- Unique constraints
    CONSTRAINT unique_email UNIQUE (email),
    CONSTRAINT unique_combo UNIQUE (date_only, time_only),

    -- Exclusion constraint
    CONSTRAINT no_overlap EXCLUDE USING gist (
        int_range WITH &&,
        timestamp_with_zone WITH =
    )
) WITH (
    fillfactor = 70,
    autovacuum_enabled = true,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_scale_factor = 0.2
)
TABLESPACE pg_default;

-- Unit 7: Table inheritance
CREATE TABLE cities (
    name text,
    population float,
    altitude int
);

CREATE TABLE capitals (
    state char(2)
) INHERITS (cities);

-- Unit 8: Partitioned table with all partition strategies
CREATE TABLE measurement (
    city_id int NOT NULL,
    logdate date NOT NULL,
    peaktemp int,
    unitsales int
) PARTITION BY RANGE (logdate);

-- Unit 9: Create partitions
CREATE TABLE measurement_y2023m01 PARTITION OF measurement
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE measurement_y2023m02 PARTITION OF measurement
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01')
    PARTITION BY LIST (city_id);

CREATE TABLE measurement_y2023m02_city1 PARTITION OF measurement_y2023m02
    FOR VALUES IN (1, 2, 3);

-- Unit 10: Hash partitioned table
CREATE TABLE users_partitioned (
    user_id bigint,
    username text,
    created_at timestamptz
) PARTITION BY HASH (user_id);

CREATE TABLE users_part0 PARTITION OF users_partitioned
    FOR VALUES WITH (modulus 4, remainder 0);

CREATE TABLE users_part1 PARTITION OF users_partitioned
    FOR VALUES WITH (modulus 4, remainder 1);

-- Unit 11: List partitioned table
CREATE TABLE sales_region (
    id bigserial,
    region text,
    amount numeric
) PARTITION BY LIST (region);

CREATE TABLE sales_north PARTITION OF sales_region
    FOR VALUES IN ('north', 'northeast', 'northwest');

CREATE TABLE sales_south PARTITION OF sales_region
    FOR VALUES IN ('south', 'southeast', 'southwest');

CREATE TABLE sales_other PARTITION OF sales_region DEFAULT;

-- Unit 12: Foreign tables
CREATE SERVER foreign_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'remote.example.com', dbname 'foreign_db', port '5432');

CREATE USER MAPPING FOR postgres
    SERVER foreign_server
    OPTIONS (user 'foreign_user', password 'secret');

CREATE FOREIGN TABLE foreign_users (
    id integer NOT NULL,
    username text,
    email text
) SERVER foreign_server
OPTIONS (schema_name 'public', table_name 'users');

-- Unit 13: Unlogged and temporary tables
CREATE UNLOGGED TABLE unlogged_data (
    id serial,
    data jsonb,
    created_at timestamptz DEFAULT now()
);

CREATE TEMP TABLE temp_calculations (
    calc_id serial,
    result numeric,
    computed_at timestamptz
) ON COMMIT DROP;

CREATE TEMPORARY TABLE session_data (
    key text PRIMARY KEY,
    value jsonb
) ON COMMIT DELETE ROWS;

-- Unit 14: Complex indexes with all types
CREATE UNIQUE INDEX idx_unique_email
    ON everything_table (email)
    WHERE is_active = true;

CREATE INDEX CONCURRENTLY idx_json_gin
    ON everything_table USING gin (jsonb_data);

CREATE INDEX idx_array_gin
    ON everything_table USING gin (text_array);

CREATE INDEX idx_trgm
    ON everything_table USING gin (text_unlimited gin_trgm_ops);

CREATE INDEX idx_gist_range
    ON everything_table USING gist (int_range);

CREATE INDEX idx_sp_gist
    ON everything_table USING spgist (point_2d);

CREATE INDEX idx_brin
    ON everything_table USING brin (timestamp_with_zone);

CREATE INDEX idx_bloom
    ON everything_table USING bloom (id, normal_int, big_int)
    WITH (length=80, col1=2, col2=2, col3=4);

CREATE INDEX idx_hash
    ON everything_table USING hash (varchar_limited);

CREATE INDEX idx_btree_multicolumn
    ON everything_table (date_only DESC NULLS LAST, time_only ASC NULLS FIRST);

CREATE INDEX idx_expression
    ON everything_table (lower(text_unlimited));

CREATE INDEX idx_partial
    ON everything_table (jsonb_data -> 'name')
    WHERE jsonb_data @> '{"active": true}';

-- Unit 15: Foreign key relationships with all options
CREATE TABLE parent_table (
    id serial PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE child_table (
    id serial PRIMARY KEY,
    parent_id integer NOT NULL,
    value text,
    CONSTRAINT fk_parent
        FOREIGN KEY (parent_id)
        REFERENCES parent_table(id)
        ON DELETE CASCADE
        ON UPDATE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
);

-- Unit 16: Views with all complexity levels
CREATE VIEW simple_view AS
    SELECT id, email, is_active
    FROM everything_table;

CREATE OR REPLACE VIEW complex_view AS
    WITH RECURSIVE cte AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1 FROM cte WHERE n < 100
    )
    SELECT
        e.*,
        cte.n,
        CASE
            WHEN e.normal_int > 100 THEN 'high'
            WHEN e.normal_int > 50 THEN 'medium'
            ELSE 'low'
        END AS category,
        ROW_NUMBER() OVER (PARTITION BY e.is_active ORDER BY e.timestamp_with_zone DESC) AS rn,
        LAG(e.price, 1) OVER (ORDER BY e.date_only) AS prev_price,
        LEAD(e.price, 1) OVER (ORDER BY e.date_only) AS next_price
    FROM everything_table e
    CROSS JOIN cte
    WHERE e.jsonb_data ? 'key'
        AND e.timestamp_with_zone >= CURRENT_DATE - INTERVAL '30 days';

-- Unit 17: Materialized views
CREATE MATERIALIZED VIEW mat_view_summary AS
    SELECT
        date_trunc('hour', timestamp_with_zone) AS hour,
        COUNT(*) AS record_count,
        AVG(normal_int)::numeric(10,2) AS avg_value,
        jsonb_agg(jsonb_data) AS aggregated_data
    FROM everything_table
    GROUP BY date_trunc('hour', timestamp_with_zone)
WITH NO DATA;

CREATE UNIQUE INDEX ON mat_view_summary (hour);
REFRESH MATERIALIZED VIEW CONCURRENTLY mat_view_summary;

-- Unit 18: Stored procedures with all parameter modes
CREATE OR REPLACE PROCEDURE complex_procedure(
    IN input_param integer,
    INOUT inout_param text,
    OUT out_param timestamp,
    VARIADIC numbers integer[] DEFAULT '{}'::integer[]
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    local_var integer := 10;
    record_var RECORD;
    cursor_var CURSOR FOR SELECT * FROM everything_table;
BEGIN
    -- Raise notice
    RAISE NOTICE 'Starting procedure with input: %', input_param;

    -- Exception handling
    BEGIN
        -- Transaction control
        COMMIT;

        -- Loop examples
        FOR i IN 1..10 LOOP
            CONTINUE WHEN i = 5;
            EXIT WHEN i = 8;
            local_var := local_var + i;
        END LOOP;

        -- Cursor operations
        OPEN cursor_var;
        FETCH cursor_var INTO record_var;
        CLOSE cursor_var;

        -- Dynamic SQL
        EXECUTE format('SELECT $1::text || %L', inout_param)
        INTO inout_param
        USING input_param;

        out_param := CURRENT_TIMESTAMP;

    EXCEPTION
        WHEN division_by_zero THEN
            RAISE WARNING 'Division by zero caught';
        WHEN OTHERS THEN
            RAISE LOG 'Error: % %', SQLSTATE, SQLERRM;
            ROLLBACK;
    END;
END;
$$;

-- Unit 19: Functions with all return types
CREATE OR REPLACE FUNCTION polymorphic_function(
    anyelement,
    anyelement
) RETURNS anyelement
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
COST 100
AS $$
    SELECT CASE
        WHEN $1 IS NULL THEN $2
        WHEN $2 IS NULL THEN $1
        ELSE $1
    END;
$$;

-- Unit 20: Trigger function
CREATE OR REPLACE FUNCTION trigger_function()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    audit_id bigint;
BEGIN
    -- Check trigger event
    IF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, operation, user_name, changed_at)
        VALUES (TG_TABLE_NAME, TG_OP, current_user, CURRENT_TIMESTAMP);
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        -- Check specific column changes
        IF NEW.email IS DISTINCT FROM OLD.email THEN
            RAISE NOTICE 'Email changed from % to %', OLD.email, NEW.email;
        END IF;
        NEW.timestamp_with_zone = CURRENT_TIMESTAMP;
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        NEW.unique_id = uuid_generate_v4();
        RETURN NEW;
    END IF;

    RETURN NULL;
END;
$$;

-- Unit 21: Create triggers with all timing options
CREATE TRIGGER before_insert_trigger
    BEFORE INSERT ON everything_table
    FOR EACH ROW
    EXECUTE FUNCTION trigger_function();

CREATE TRIGGER after_update_trigger
    AFTER UPDATE OF email, is_active ON everything_table
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE FUNCTION trigger_function();

CREATE TRIGGER instead_of_trigger
    INSTEAD OF DELETE ON simple_view
    FOR EACH ROW
    EXECUTE FUNCTION trigger_function();

CREATE CONSTRAINT TRIGGER deferred_trigger
    AFTER INSERT OR UPDATE ON everything_table
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION trigger_function();

-- Unit 22: Event triggers
CREATE OR REPLACE FUNCTION event_trigger_function()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Event trigger fired: %', TG_EVENT;
END;
$$;

CREATE EVENT TRIGGER ddl_trigger
    ON ddl_command_start
    WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE')
    EXECUTE FUNCTION event_trigger_function();

-- Unit 23: Rules
CREATE RULE insert_rule AS
    ON INSERT TO simple_view
    DO INSTEAD
    INSERT INTO everything_table (id, email, is_active)
    VALUES (NEW.id, NEW.email, NEW.is_active);

CREATE RULE update_rule AS
    ON UPDATE TO simple_view
    WHERE OLD.is_active = true
    DO INSTEAD NOTHING;

-- Unit 24: Row-level security
ALTER TABLE everything_table ENABLE ROW LEVEL SECURITY;

CREATE POLICY select_policy ON everything_table
    FOR SELECT
    USING (is_active = true);

CREATE POLICY insert_policy ON everything_table
    FOR INSERT
    WITH CHECK (email IS NOT NULL);

CREATE POLICY update_policy ON everything_table
    FOR UPDATE
    USING (current_user = 'postgres')
    WITH CHECK (is_active = true);

CREATE POLICY delete_policy ON everything_table
    FOR DELETE
    USING (timestamp_with_zone < CURRENT_DATE - INTERVAL '1 year');

-- Unit 25: Complex CTE queries
WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1) AS (
    SELECT
        1::bigint AS n,
        0::bigint AS fib_n,
        1::bigint AS fib_n_plus_1
    UNION ALL
    SELECT
        n + 1,
        fib_n_plus_1,
        fib_n + fib_n_plus_1
    FROM fibonacci
    WHERE n < 20
),
aggregated AS (
    SELECT
        jsonb_data,
        COUNT(*) OVER (PARTITION BY is_active) AS count_per_group,
        RANK() OVER (ORDER BY normal_int DESC) AS rank_val,
        DENSE_RANK() OVER (ORDER BY normal_int DESC) AS dense_rank_val,
        PERCENT_RANK() OVER (ORDER BY normal_int) AS percent_rank_val,
        CUME_DIST() OVER (ORDER BY normal_int) AS cume_dist_val,
        NTILE(4) OVER (ORDER BY normal_int) AS quartile,
        FIRST_VALUE(email) OVER (
            PARTITION BY is_active
            ORDER BY timestamp_with_zone
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS first_email,
        LAST_VALUE(email) OVER (
            PARTITION BY is_active
            ORDER BY timestamp_with_zone
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS last_email,
        NTH_VALUE(email, 2) OVER (
            PARTITION BY is_active
            ORDER BY timestamp_with_zone
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS second_email
    FROM everything_table
),
lateral_join AS (
    SELECT
        e.*,
        l.sum_value
    FROM everything_table e,
    LATERAL (
        SELECT SUM(normal_int) AS sum_value
        FROM everything_table e2
        WHERE e2.date_only = e.date_only
    ) l
)
SELECT * FROM fibonacci
UNION ALL
SELECT n, 0, 0 FROM aggregated LIMIT 10;

-- Unit 26: Window functions with all frame specifications
SELECT
    id,
    normal_int,
    SUM(normal_int) OVER w1 AS running_sum,
    AVG(normal_int) OVER w2 AS moving_avg,
    COUNT(*) FILTER (WHERE is_active) OVER w3 AS active_count,
    STRING_AGG(email, ', ') OVER w4 AS emails,
    ARRAY_AGG(jsonb_data ORDER BY timestamp_with_zone) OVER w5 AS json_array,
    JSON_AGG(inventory ORDER BY id) OVER w6 AS json_aggregation,
    JSONB_AGG(DISTINCT jsonb_data) OVER w7 AS unique_json,
    MODE() WITHIN GROUP (ORDER BY normal_int) OVER w8 AS mode_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY normal_int) OVER w9 AS median,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY normal_int) OVER w10 AS percentile_95
FROM everything_table
WINDOW
    w1 AS (ORDER BY date_only ROWS UNBOUNDED PRECEDING),
    w2 AS (ORDER BY date_only ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING),
    w3 AS (PARTITION BY date_only),
    w4 AS (PARTITION BY is_active ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING),
    w5 AS (ORDER BY id RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW),
    w6 AS (ORDER BY id GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING),
    w7 AS (PARTITION BY date_only ORDER BY id),
    w8 AS (PARTITION BY is_active),
    w9 AS (ORDER BY id),
    w10 AS (ORDER BY id EXCLUDE CURRENT ROW);

-- Unit 27: MERGE statement (SQL:2003 standard)
MERGE INTO everything_table AS target
USING (
    SELECT * FROM everything_table WHERE is_active = true
) AS source ON target.id = source.id
WHEN MATCHED AND target.timestamp_with_zone < source.timestamp_with_zone THEN
    UPDATE SET
        jsonb_data = source.jsonb_data || '{"merged": true}'::jsonb,
        timestamp_with_zone = CURRENT_TIMESTAMP
WHEN MATCHED THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT (email, is_active)
    VALUES (source.email, true);

-- Unit 28: INSERT with all options
INSERT INTO everything_table (email, jsonb_data)
VALUES
    ('test1@example.com', '{"name": "Test 1"}'::jsonb),
    ('test2@example.com', '{"name": "Test 2"}'::jsonb)
ON CONFLICT (email)
DO UPDATE SET
    jsonb_data = EXCLUDED.jsonb_data || everything_table.jsonb_data,
    timestamp_with_zone = CASE
        WHEN everything_table.is_active THEN CURRENT_TIMESTAMP
        ELSE everything_table.timestamp_with_zone
    END
RETURNING id, email, jsonb_data;

-- Unit 29: UPDATE with complex conditions
UPDATE everything_table AS e
SET
    jsonb_data = jsonb_set(
        jsonb_data,
        '{metadata, updated}',
        to_jsonb(CURRENT_TIMESTAMP),
        true
    ),
    integer_array[1] = 100,
    text_array = array_append(text_array, 'new_item'),
    normal_int = normal_int + 1
FROM (
    SELECT id, AVG(normal_int) AS avg_val
    FROM everything_table
    GROUP BY id
) AS subquery
WHERE e.id = subquery.id
    AND e.normal_int < subquery.avg_val
    AND e.jsonb_data @> '{"active": true}'
    AND e.text_unlimited ~* 'pattern'
    AND e.integer_array && ARRAY[1,2,3]
    AND e.ip_address << '192.168.0.0/16'::cidr
RETURNING e.*;

-- Unit 30: DELETE with USING clause
DELETE FROM everything_table AS e
USING parent_table AS p
WHERE e.normal_int = p.id
    AND e.timestamp_with_zone < CURRENT_DATE - INTERVAL '1 year'
    AND NOT EXISTS (
        SELECT 1 FROM child_table c
        WHERE c.parent_id = p.id
    )
RETURNING e.id, e.email;

-- Unit 31: COPY operations
COPY everything_table (email, jsonb_data)
FROM '/tmp/data.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    NULL '\N',
    QUOTE '"',
    ESCAPE '\',
    FORCE_NOT_NULL (email),
    ENCODING 'UTF8'
);

COPY (
    SELECT email, jsonb_data
    FROM everything_table
    WHERE is_active = true
) TO '/tmp/export.csv'
WITH (FORMAT csv, HEADER true);

-- Unit 32: Transaction control with savepoints
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

SAVEPOINT sp1;
UPDATE everything_table SET normal_int = normal_int + 1;

SAVEPOINT sp2;
DELETE FROM everything_table WHERE is_active = false;

ROLLBACK TO SAVEPOINT sp1;
RELEASE SAVEPOINT sp1;

COMMIT;

-- Unit 33: Advisory locks
SELECT pg_advisory_lock(12345);
SELECT pg_advisory_lock_shared(12345);
SELECT pg_try_advisory_lock(12345);
SELECT pg_advisory_unlock(12345);
SELECT pg_advisory_unlock_all();

-- Advisory locks with two keys
SELECT pg_advisory_lock(1, 2);
SELECT pg_advisory_xact_lock(1, 2);

-- Unit 34: LISTEN/NOTIFY
LISTEN channel_name;
NOTIFY channel_name, 'payload message';
UNLISTEN channel_name;
UNLISTEN *;

-- Unit 35: Prepared statements
PREPARE prepared_select (integer, boolean) AS
    SELECT * FROM everything_table
    WHERE normal_int = $1 AND is_active = $2;

EXECUTE prepared_select(100, true);
DEALLOCATE prepared_select;

-- Unit 36: Cursors
BEGIN;

DECLARE cursor_name CURSOR WITH HOLD FOR
    SELECT * FROM everything_table
    ORDER BY id;

DECLARE scroll_cursor SCROLL CURSOR FOR
    SELECT * FROM everything_table;

FETCH FORWARD 10 FROM cursor_name;
FETCH BACKWARD 5 FROM scroll_cursor;
FETCH ABSOLUTE 100 FROM scroll_cursor;
FETCH RELATIVE -10 FROM scroll_cursor;
FETCH FIRST FROM scroll_cursor;
FETCH LAST FROM scroll_cursor;

MOVE FORWARD 10 IN cursor_name;

CLOSE cursor_name;
CLOSE scroll_cursor;

COMMIT;

-- Unit 37: Full-text search operations
SELECT
    *,
    ts_rank(search_vector, query) AS rank,
    ts_rank_cd(search_vector, query) AS rank_cd,
    ts_headline('english', text_unlimited, query) AS headline
FROM everything_table,
    to_tsquery('english', 'search & (term | phrase)') AS query
WHERE search_vector @@ query
ORDER BY rank DESC;

-- Update tsvector
UPDATE everything_table
SET search_vector =
    setweight(to_tsvector('english', coalesce(email, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(text_unlimited, '')), 'B');

-- Unit 38: JSON/JSONB operations
SELECT
    jsonb_data -> 'key' AS extract_field,
    jsonb_data ->> 'key' AS extract_text,
    jsonb_data #> '{nested,path}' AS extract_path,
    jsonb_data #>> '{nested,path}' AS extract_path_text,
    jsonb_data @> '{"key": "value"}' AS contains,
    jsonb_data <@ '{"parent": {}}' AS contained_by,
    jsonb_data ? 'key' AS has_key,
    jsonb_data ?& array['key1', 'key2'] AS has_all_keys,
    jsonb_data ?| array['key1', 'key2'] AS has_any_keys,
    jsonb_data || '{"new": "field"}' AS concatenate,
    jsonb_data - 'key' AS remove_key,
    jsonb_data - '{key1,key2}' AS remove_keys,
    jsonb_data #- '{nested,path}' AS remove_path,
    jsonb_pretty(jsonb_data) AS formatted,
    jsonb_typeof(jsonb_data) AS type,
    jsonb_array_length(jsonb_data) AS array_length,
    jsonb_object_keys(jsonb_data) AS object_keys,
    jsonb_array_elements(jsonb_data) AS array_elements,
    jsonb_array_elements_text(jsonb_data) AS array_elements_text,
    jsonb_each(jsonb_data) AS each_pair,
    jsonb_each_text(jsonb_data) AS each_pair_text,
    jsonb_to_record(jsonb_data) AS (field1 text, field2 int),
    jsonb_to_recordset(jsonb_data) AS (field1 text, field2 int),
    jsonb_strip_nulls(jsonb_data) AS no_nulls,
    jsonb_set(jsonb_data, '{new,path}', '"value"', true) AS set_value,
    jsonb_insert(jsonb_data, '{new,path}', '"value"') AS insert_value,
    jsonb_path_exists(jsonb_data, '$.path.to.field') AS path_exists,
    jsonb_path_match(jsonb_data, '$.field == "value"') AS path_match,
    jsonb_path_query(jsonb_data, '$.path[*] ? (@ > 5)') AS path_query,
    jsonb_path_query_array(jsonb_data, '$.path[*]') AS path_query_array,
    jsonb_path_query_first(jsonb_data, '$.path[0]') AS path_query_first
FROM everything_table;

-- Unit 39: Array operations
SELECT
    array_append(integer_array, 100) AS append,
    array_prepend(100, integer_array) AS prepend,
    array_cat(integer_array, ARRAY[200, 300]) AS concatenate,
    array_remove(integer_array, 50) AS remove,
    array_replace(integer_array, 50, 500) AS replace,
    array_position(integer_array, 50) AS position,
    array_positions(integer_array, 50) AS positions,
    array_dims(multidim_array) AS dimensions,
    array_ndims(multidim_array) AS num_dimensions,
    array_length(integer_array, 1) AS length,
    array_lower(integer_array, 1) AS lower_bound,
    array_upper(integer_array, 1) AS upper_bound,
    cardinality(integer_array) AS cardinality,
    array_to_string(text_array, ', ') AS to_string,
    string_to_array('a,b,c', ',') AS from_string,
    unnest(integer_array) AS unnested,
    integer_array[1] AS element_access,
    integer_array[1:3] AS slice,
    integer_array || ARRAY[400] AS concat_operator,
    integer_array && ARRAY[1,2] AS overlap,
    integer_array @> ARRAY[1] AS contains,
    integer_array <@ ARRAY[1,2,3,4,5] AS contained_by,
    array_fill(0, ARRAY[3,3]) AS filled_array,
    array_agg(DISTINCT normal_int ORDER BY normal_int) AS aggregated
FROM everything_table
WHERE 50 = ANY(integer_array)
    AND 100 = ALL(integer_array)
    AND 200 = SOME(integer_array);

-- Unit 40: Range operations
SELECT
    int4range(1, 10) AS int_range_create,
    numrange(1.5, 2.5, '[]') AS num_range_inclusive,
    daterange('2023-01-01', '2023-12-31', '[)') AS date_range_half_open,
    lower(int_range) AS lower_bound,
    upper(int_range) AS upper_bound,
    isempty(int_range) AS is_empty,
    lower_inc(int_range) AS lower_inclusive,
    upper_inc(int_range) AS upper_inclusive,
    lower_inf(int_range) AS lower_infinite,
    upper_inf(int_range) AS upper_infinite,
    int_range @> 5 AS contains_element,
    int_range @> int4range(2,4) AS contains_range,
    int_range <@ int4range(0,100) AS contained_by,
    int_range && int4range(5,15) AS overlaps,
    int_range << int4range(20,30) AS strictly_left,
    int_range >> int4range(0,1) AS strictly_right,
    int_range &< int4range(10,20) AS not_extends_right,
    int_range &> int4range(0,5) AS not_extends_left,
    int_range + int4range(5,15) AS union,
    int_range * int4range(5,15) AS intersection,
    int_range - int4range(5,15) AS difference,
    range_merge(int_range, int4range(10,20)) AS merged
FROM everything_table;

-- Unit 41: Aggregate functions with all options
SELECT
    COUNT(*) AS total_count,
    COUNT(DISTINCT email) AS unique_emails,
    COUNT(*) FILTER (WHERE is_active = true) AS active_count,
    SUM(normal_int) AS sum_val,
    AVG(normal_int)::numeric(10,2) AS avg_val,
    MIN(normal_int) AS min_val,
    MAX(normal_int) AS max_val,
    STDDEV(normal_int) AS std_dev,
    STDDEV_POP(normal_int) AS std_dev_pop,
    STDDEV_SAMP(normal_int) AS std_dev_samp,
    VARIANCE(normal_int) AS variance,
    VAR_POP(normal_int) AS var_pop,
    VAR_SAMP(normal_int) AS var_samp,
    COVAR_POP(normal_int, big_int) AS covar_pop,
    COVAR_SAMP(normal_int, big_int) AS covar_samp,
    CORR(normal_int, big_int) AS correlation,
    REGR_AVGX(normal_int, big_int) AS regr_avgx,
    REGR_AVGY(normal_int, big_int) AS regr_avgy,
    REGR_COUNT(normal_int, big_int) AS regr_count,
    REGR_INTERCEPT(normal_int, big_int) AS regr_intercept,
    REGR_R2(normal_int, big_int) AS regr_r2,
    REGR_SLOPE(normal_int, big_int) AS regr_slope,
    REGR_SXX(normal_int, big_int) AS regr_sxx,
    REGR_SXY(normal_int, big_int) AS regr_sxy,
    REGR_SYY(normal_int, big_int) AS regr_syy,
    BIT_AND(normal_int) AS bit_and,
    BIT_OR(normal_int) AS bit_or,
    BIT_XOR(normal_int) AS bit_xor,
    BOOL_AND(is_active) AS bool_and,
    BOOL_OR(is_active) AS bool_or,
    EVERY(is_active) AS every_true
FROM everything_table
GROUP BY GROUPING SETS (
    (is_active),
    (date_only),
    (is_active, date_only),
    ()
)
HAVING COUNT(*) > 10
    AND AVG(normal_int) > 50;

-- Unit 42: ROLLUP and CUBE
SELECT
    is_active,
    date_only,
    extract(year from timestamp_with_zone) AS year,
    COUNT(*) AS count,
    GROUPING(is_active) AS grouping_is_active,
    GROUPING(date_only) AS grouping_date,
    GROUPING(extract(year from timestamp_with_zone)) AS grouping_year
FROM everything_table
GROUP BY ROLLUP (is_active, date_only, extract(year from timestamp_with_zone));

SELECT
    is_active,
    date_only,
    COUNT(*) AS count
FROM everything_table
GROUP BY CUBE (is_active, date_only);

-- Unit 43: Set operations
SELECT email FROM everything_table WHERE is_active = true
UNION
SELECT email FROM everything_table WHERE is_active = false;

SELECT email FROM everything_table WHERE normal_int > 50
UNION ALL
SELECT email FROM everything_table WHERE normal_int <= 50;

SELECT email FROM everything_table WHERE is_active = true
INTERSECT
SELECT email FROM everything_table WHERE normal_int > 100;

SELECT email FROM everything_table WHERE is_active = true
INTERSECT ALL
SELECT email FROM everything_table WHERE date_only = CURRENT_DATE;

SELECT email FROM everything_table
EXCEPT
SELECT email FROM everything_table WHERE is_active = false;

SELECT email FROM everything_table
EXCEPT ALL
SELECT email FROM everything_table WHERE jsonb_data ? 'inactive';

-- Unit 44: VALUES clause
INSERT INTO everything_table (email, normal_int)
VALUES
    ('test1@example.com', 100),
    ('test2@example.com', 200),
    ('test3@example.com', 300);

SELECT * FROM (
    VALUES
        (1, 'one'),
        (2, 'two'),
        (3, 'three')
) AS t(num, word);

-- Unit 45: LATERAL joins
SELECT
    e1.*,
    lat.avg_value,
    lat.max_value
FROM everything_table e1,
LATERAL (
    SELECT
        AVG(e2.normal_int) AS avg_value,
        MAX(e2.normal_int) AS max_value
    FROM everything_table e2
    WHERE e2.date_only = e1.date_only
        AND e2.id != e1.id
) lat
WHERE lat.avg_value > 50;

-- Unit 46: CROSS JOIN and other join types
SELECT *
FROM everything_table e1
CROSS JOIN parent_table p;

SELECT *
FROM everything_table e1
NATURAL JOIN everything_table e2;

SELECT *
FROM everything_table e1
INNER JOIN parent_table p ON e1.normal_int = p.id;

SELECT *
FROM everything_table e1
LEFT JOIN parent_table p ON e1.normal_int = p.id;

SELECT *
FROM everything_table e1
RIGHT JOIN parent_table p ON e1.normal_int = p.id;

SELECT *
FROM everything_table e1
FULL OUTER JOIN parent_table p ON e1.normal_int = p.id;

SELECT *
FROM everything_table e1
LEFT JOIN parent_table p ON e1.normal_int = p.id
    AND p.name LIKE 'A%';

-- Unit 47: System information functions
SELECT
    current_database() AS db,
    current_schema() AS schema,
    current_schemas(true) AS schemas,
    current_user AS user,
    session_user AS session,
    version() AS version,
    pg_backend_pid() AS pid,
    pg_postmaster_start_time() AS start_time,
    pg_conf_load_time() AS config_time,
    pg_is_in_recovery() AS in_recovery,
    pg_last_wal_receive_lsn() AS wal_receive,
    pg_last_wal_replay_lsn() AS wal_replay,
    pg_last_xact_replay_timestamp() AS xact_replay,
    txid_current() AS txid,
    txid_current_if_assigned() AS txid_if_assigned,
    txid_current_snapshot() AS txid_snapshot,
    pg_snapshot_xmin(pg_current_snapshot()) AS snapshot_xmin,
    pg_snapshot_xmax(pg_current_snapshot()) AS snapshot_xmax,
    pg_visible_in_snapshot(1000, pg_current_snapshot()) AS visible;

-- Unit 48: Generate series and other generators
SELECT * FROM generate_series(1, 10);
SELECT * FROM generate_series('2023-01-01'::date, '2023-12-31'::date, '1 month'::interval);
SELECT * FROM generate_series(1, 10, 2);

SELECT * FROM generate_subscripts(ARRAY[1,2,3,4,5], 1);
SELECT * FROM generate_subscripts(ARRAY[1,2,3,4,5], 1, true);

SELECT * FROM unnest(ARRAY[1,2,3], ARRAY['a','b','c']) AS t(num, letter);

SELECT * FROM regexp_split_to_table('hello world', '\s+');
SELECT * FROM regexp_split_to_array('hello,world', ',');

-- Unit 49: Table sampling
SELECT * FROM everything_table
TABLESAMPLE BERNOULLI (10);

SELECT * FROM everything_table
TABLESAMPLE SYSTEM (10);

SELECT * FROM everything_table
TABLESAMPLE BERNOULLI (10) REPEATABLE (42);

SELECT * FROM everything_table
TABLESAMPLE SYSTEM (10) REPEATABLE (42);

-- Unit 50: Comments and documentation
COMMENT ON TABLE everything_table IS 'Table containing all PostgreSQL data types';
COMMENT ON COLUMN everything_table.email IS 'User email address with validation';
COMMENT ON INDEX idx_unique_email IS 'Unique index on active user emails';
COMMENT ON CONSTRAINT check_positive ON everything_table IS 'Ensures positive values';
COMMENT ON FUNCTION trigger_function() IS 'Audit trigger function';
COMMENT ON TRIGGER before_insert_trigger ON everything_table IS 'Pre-insert validation';
COMMENT ON VIEW complex_view IS 'Complex view with CTEs and window functions';
COMMENT ON SCHEMA analytics IS 'Analytics schema for reporting';
COMMENT ON TYPE mood IS 'User mood enumeration';
COMMENT ON DOMAIN email_address IS 'Email validation domain';

-- Unit 51: Security and permissions
CREATE ROLE read_only_user WITH LOGIN PASSWORD 'secret' VALID UNTIL '2024-12-31';
CREATE ROLE app_user WITH LOGIN PASSWORD 'app_secret' CONNECTION LIMIT 10;
CREATE ROLE admin_user WITH LOGIN SUPERUSER CREATEDB CREATEROLE REPLICATION BYPASSRLS;

ALTER ROLE read_only_user SET search_path TO public, analytics;
ALTER ROLE app_user SET statement_timeout TO '5min';

GRANT CONNECT ON DATABASE devils_advocate TO read_only_user;
GRANT USAGE ON SCHEMA public TO read_only_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO read_only_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO read_only_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO app_user;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE devils_advocate FROM PUBLIC;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO read_only_user;

ALTER DEFAULT PRIVILEGES FOR ROLE app_user IN SCHEMA public
    GRANT ALL ON TABLES TO admin_user;

-- Unit 52: Analyze and maintenance
ANALYZE everything_table;
ANALYZE everything_table (email, jsonb_data);

VACUUM everything_table;
VACUUM ANALYZE everything_table;
VACUUM FULL everything_table;
VACUUM FREEZE everything_table;

CLUSTER everything_table USING idx_btree_multicolumn;

REINDEX TABLE everything_table;
REINDEX INDEX idx_unique_email;
REINDEX DATABASE devils_advocate;

-- Unit 53: Table and index statistics
SELECT
    schemaname,
    tablename,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE tablename = 'everything_table';

SELECT
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename = 'everything_table';

-- Unit 54: Query optimization hints
/*+ SeqScan(everything_table) */
SELECT * FROM everything_table WHERE normal_int = 100;

/*+ IndexScan(everything_table idx_btree_multicolumn) */
SELECT * FROM everything_table WHERE date_only = CURRENT_DATE;

/*+ HashJoin(e p) */
SELECT * FROM everything_table e
JOIN parent_table p ON e.normal_int = p.id;

/*+ Leading(p e) */
SELECT * FROM everything_table e
JOIN parent_table p ON e.normal_int = p.id;

-- Unit 55: Final cleanup
DROP TRIGGER IF EXISTS before_insert_trigger ON everything_table;
DROP FUNCTION IF EXISTS trigger_function() CASCADE;
DROP PROCEDURE IF EXISTS complex_procedure CASCADE;
DROP VIEW IF EXISTS complex_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_view_summary CASCADE;
DROP TABLE IF EXISTS everything_table CASCADE;
DROP SCHEMA IF EXISTS analytics CASCADE;
DROP TYPE IF EXISTS mood CASCADE;
DROP DOMAIN IF EXISTS email_address CASCADE;
DROP EXTENSION IF EXISTS "uuid-ossp" CASCADE;
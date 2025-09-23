-- Simple PostgreSQL schema for testing
-- Contains common patterns and incompatibilities

SET client_encoding = 'UTF8';
SET search_path = public;

CREATE TABLE users (
    id integer NOT NULL,
    email varchar(255) NOT NULL,
    name text,
    tags text[],
    created_at timestamp with time zone DEFAULT now(),
    score integer DEFAULT -1,
    profile_data jsonb,
    CONSTRAINT pk_users PRIMARY KEY (id),
    CONSTRAINT uq_users_email UNIQUE (email),
    CONSTRAINT chk_users_score CHECK (score >= -1)
);

CREATE TABLE departments (
    id integer NOT NULL,
    name varchar(100) NOT NULL,
    budget numeric(12,2),
    location point,
    CONSTRAINT pk_departments PRIMARY KEY (id)
);

CREATE TABLE user_departments (
    user_id integer NOT NULL,
    department_id integer NOT NULL,
    role varchar(50),
    hired_date date,
    CONSTRAINT pk_user_departments PRIMARY KEY (user_id, department_id),
    CONSTRAINT fk_user_departments_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_user_departments_dept FOREIGN KEY (department_id) REFERENCES departments(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX idx_users_email ON users (email);
CREATE INDEX idx_users_tags ON users USING gin (tags);
CREATE UNIQUE INDEX idx_departments_name ON departments (name) WHERE name IS NOT NULL;

CREATE SEQUENCE user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE users ALTER COLUMN id SET DEFAULT nextval('user_id_seq');
ALTER SEQUENCE user_id_seq OWNED BY users.id;

COMMENT ON TABLE users IS 'Application users';
COMMENT ON COLUMN users.email IS 'User email address';

GRANT ALL ON TABLE users TO app_role;
GRANT SELECT ON TABLE departments TO public;

SELECT pg_catalog.setval('user_id_seq', 1, false);

COPY users (id, email, name) FROM stdin;
1	admin@example.com	Administrator
2	user@example.com	Regular User
\.

-- Function definition with dollar quoting
CREATE OR REPLACE FUNCTION get_user_count()
RETURNS integer AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM users);
END;
$$ LANGUAGE plpgsql;
"""
Shared pytest fixtures for all test modules.
"""

import pytest
from pathlib import Path


@pytest.fixture
def sample_sql():
    """Sample SQL for testing."""
    return """
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    tags TEXT[],
    metadata JSONB,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""


@pytest.fixture
def complex_sql():
    """Complex SQL with various PostgreSQL features."""
    return """
-- Complex types and constraints
CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');

CREATE TABLE complex_table (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    score NUMERIC(10,2) CHECK (score >= 0 AND score <= 100),
    mood mood DEFAULT 'neutral',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    EXCLUDE USING gist (tstzrange(created_at, created_at + interval '1 hour') WITH &&)
);

CREATE INDEX idx_complex_data ON complex_table USING gin(data);
CREATE INDEX idx_complex_tags ON complex_table USING gin(tags);
"""


@pytest.fixture
def malformed_sql():
    """Malformed SQL for error testing."""
    return """
CREATE TABLE broken (
    id SERIAL PRIMARY KEY
    missing_comma VARCHAR(100)
);

CREATE TABLE orphan_fk (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER REFERENCES nonexistent_table(id)
);
"""


@pytest.fixture
def expected_dbml():
    """Expected DBML output for sample SQL."""
    return """Project postgresql_schema {
  database_type: 'PostgreSQL'
  Note: '''
    This DBML was automatically converted from PostgreSQL.
    Some features may be missing or simplified.
    See conversion report for complete details.
  '''
}

Table users [headercolor: #3498DB] {
  id integer [pk, increment]
  email varchar(255) [unique, not null]
  username varchar(100) [unique, not null]
  created_at timestamptz [default: 'CURRENT_TIMESTAMP']
}

Table posts [headercolor: #3498DB] {
  id integer [pk, increment]
  user_id integer [not null]
  title varchar(255) [not null]
  content text
  tags "text []"
  metadata json Note: 'Originally jsonb'
}

Ref: posts.user_id > users.id [delete: cascade]
"""


@pytest.fixture
def sample_schema():
    """Sample schema dictionary for testing transformations."""
    return {
        'tables': [
            {
                'name': 'users',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL', 'constraints': ['PRIMARY KEY']},
                    {'name': 'email', 'type': 'VARCHAR(255)', 'constraints': ['UNIQUE', 'NOT NULL']},
                    {'name': 'username', 'type': 'VARCHAR(100)', 'constraints': ['UNIQUE', 'NOT NULL']},
                    {'name': 'created_at', 'type': 'TIMESTAMPTZ', 'default': 'CURRENT_TIMESTAMP'}
                ]
            },
            {
                'name': 'posts',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL', 'constraints': ['PRIMARY KEY']},
                    {'name': 'user_id', 'type': 'INTEGER', 'constraints': ['NOT NULL']},
                    {'name': 'title', 'type': 'VARCHAR(255)', 'constraints': ['NOT NULL']},
                    {'name': 'content', 'type': 'TEXT', 'constraints': []},
                    {'name': 'tags', 'type': 'TEXT[]', 'constraints': []},
                    {'name': 'metadata', 'type': 'JSONB', 'constraints': []}
                ]
            }
        ],
        'foreign_keys': [
            {
                'table': 'posts',
                'column': 'user_id',
                'ref_table': 'users',
                'ref_column': 'id',
                'on_delete': 'CASCADE'
            }
        ]
    }


@pytest.fixture
def test_fixtures_dir():
    """Path to test fixtures directory."""
    return Path(__file__).parent / 'fixtures'


@pytest.fixture
def temp_output_file(tmp_path):
    """Temporary output file for testing."""
    return tmp_path / "test_output.dbml"


@pytest.fixture
def mock_logger(mocker):
    """Mock logger for testing."""
    return mocker.Mock()
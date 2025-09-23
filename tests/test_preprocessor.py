"""
Tests for SQL preprocessing functionality.
"""

import pytest
from src.preprocessor.sql_cleaner import SQLCleaner


class TestSQLCleaner:
    """Test SQL cleaning and preprocessing."""

    def setup_method(self):
        """Set up test fixtures."""
        self.cleaner = SQLCleaner()

    def test_remove_set_statements(self):
        """Test removal of SET statements."""
        sql_with_sets = """
        SET client_encoding = 'UTF8';
        SET search_path = public;
        CREATE TABLE users (id integer);
        """

        cleaned = self.cleaner.clean_dump(sql_with_sets)

        assert 'SET client_encoding' not in cleaned
        assert 'SET search_path' not in cleaned
        assert 'CREATE TABLE users' in cleaned

    def test_remove_comment_statements(self):
        """Test removal of COMMENT ON statements."""
        sql_with_comments = """
        CREATE TABLE users (id integer);
        COMMENT ON TABLE users IS 'User data';
        COMMENT ON COLUMN users.id IS 'Primary key';
        """

        cleaned = self.cleaner.clean_dump(sql_with_comments)

        assert 'COMMENT ON TABLE' not in cleaned
        assert 'COMMENT ON COLUMN' not in cleaned
        assert 'CREATE TABLE users' in cleaned

    def test_remove_copy_statements(self):
        """Test removal of COPY statements."""
        sql_with_copy = """
        CREATE TABLE users (id integer);
        COPY users FROM stdin;
        1
        2
        \.
        """

        cleaned = self.cleaner.clean_dump(sql_with_copy)

        assert 'COPY users FROM' not in cleaned
        assert 'CREATE TABLE users' in cleaned

    def test_fix_negative_defaults(self):
        """Test fixing negative default values."""
        sql_with_negative = """
        CREATE TABLE test (
            id integer DEFAULT -1,
            score integer DEFAULT -999
        );
        """

        cleaned = self.cleaner.clean_dump(sql_with_negative)

        assert "DEFAULT '-1'" in cleaned
        assert "DEFAULT '-999'" in cleaned
        assert 'DEFAULT -1' not in cleaned

    def test_fix_uuid_functions(self):
        """Test fixing UUID function calls."""
        sql_with_uuid = """
        CREATE TABLE users (
            id uuid DEFAULT gen_random_uuid()
        );
        """

        cleaned = self.cleaner.clean_dump(sql_with_uuid)

        assert '`uuid_generate_v4()`' in cleaned
        assert 'gen_random_uuid()' not in cleaned

    def test_convert_multiword_types(self):
        """Test conversion of multi-word types."""
        sql_with_multiword = """
        CREATE TABLE test (
            created_at timestamp with time zone,
            updated_at timestamp without time zone,
            score double precision
        );
        """

        cleaned = self.cleaner.clean_dump(sql_with_multiword)

        assert 'timestamptz' in cleaned
        assert 'timestamp' in cleaned
        assert 'float8' in cleaned
        assert 'timestamp with time zone' not in cleaned
        assert 'double precision' not in cleaned

    def test_quote_array_types(self):
        """Test quoting of array types."""
        sql_with_arrays = """
        CREATE TABLE test (
            tags text[],
            scores integer[]
        );
        """

        cleaned = self.cleaner.clean_dump(sql_with_arrays)

        assert '"text []"' in cleaned
        assert '"integer []"' in cleaned
        assert 'text[]' not in cleaned

    def test_get_removal_report(self):
        """Test generation of removal report."""
        sql_with_removes = """
        SET client_encoding = 'UTF8';
        CREATE TABLE users (id integer);
        COMMENT ON TABLE users IS 'test';
        GRANT ALL ON users TO public;
        """

        self.cleaner.clean_dump(sql_with_removes)
        report = self.cleaner.get_removal_report()

        assert report['total_removed'] >= 3
        assert 'SET' in report['by_type']
        assert 'COMMENT' in report['by_type']
        assert 'PERMISSION' in report['by_type']
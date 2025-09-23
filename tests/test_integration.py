"""
Integration tests for the complete conversion pipeline.
"""

import pytest
from pathlib import Path
import tempfile
import os

from src.preprocessor.sql_cleaner import SQLCleaner
from src.parser.sql_parser import SQLParser
from src.transformer.type_mapper import TypeMapper
from src.transformer.constraint_handler import ConstraintHandler
from src.generator.dbml_generator import DBMLGenerator
from src.quality.silent_failure_detector import SilentFailureDetector
from src.quality.six_sigma_metrics import SixSigmaMetrics


class TestIntegration:
    """Test complete conversion pipeline."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = Path(__file__).parent
        self.fixtures_dir = self.test_dir / 'fixtures'
        self.sample_sql_path = self.fixtures_dir / 'sample_dumps' / 'simple_schema.sql'

    def test_complete_conversion_pipeline(self):
        """Test the complete conversion pipeline from SQL to DBML."""

        # Load test SQL file
        with open(self.sample_sql_path, 'r') as f:
            sql_content = f.read()

        # Phase 1: Preprocessing
        cleaner = SQLCleaner()
        cleaned_sql = cleaner.clean_dump(sql_content)

        # Verify preprocessing removed problematic statements
        assert 'SET client_encoding' not in cleaned_sql
        assert 'COMMENT ON' not in cleaned_sql
        assert 'COPY users' not in cleaned_sql
        assert 'GRANT ALL' not in cleaned_sql

        # Verify type fixes were applied
        assert 'timestamptz' in cleaned_sql  # timestamp with time zone -> timestamptz
        assert '"text []"' in cleaned_sql    # text[] -> "text []"
        assert "DEFAULT '-1'" in cleaned_sql # DEFAULT -1 -> DEFAULT '-1'

        # Phase 2: Parsing
        parser = SQLParser()
        schema_data = parser.parse_sql_dump(cleaned_sql)

        # Verify parsing extracted key elements
        assert len(schema_data['tables']) == 3
        table_names = {t['table_name'] for t in schema_data['tables']}
        assert table_names == {'users', 'departments', 'user_departments'}

        # Verify relationships were extracted
        assert len(schema_data['relationships']) >= 2

        # Phase 3: Type transformation
        type_mapper = TypeMapper()
        transformed_schema = type_mapper.transform_types(schema_data)

        # Verify array types were quoted
        users_table = next(t for t in transformed_schema['tables'] if t['table_name'] == 'users')
        tags_column = next(c for c in users_table['columns'] if c['column_name'] == 'tags')
        assert tags_column['data_type'] == '"text []"'

        # Verify geometric types were converted to text
        dept_table = next(t for t in transformed_schema['tables'] if t['table_name'] == 'departments')
        location_column = next(c for c in dept_table['columns'] if c['column_name'] == 'location')
        assert location_column['data_type'] == 'text'

        # Phase 4: Constraint handling
        constraint_handler = ConstraintHandler()
        processed_schema = constraint_handler.process_constraints(transformed_schema)

        # Verify CHECK constraints were dropped
        users_constraints = users_table['constraints']
        check_constraints = [c for c in users_constraints if c.get('constraint_type') == 'c']
        assert len(check_constraints) == 0  # Should be removed

        # Phase 5: DBML generation
        generator = DBMLGenerator()
        dbml_content = generator.generate(processed_schema)

        # Verify DBML content structure
        assert 'Project postgresql_schema' in dbml_content
        assert 'Table users' in dbml_content
        assert 'Table departments' in dbml_content
        assert 'Table user_departments' in dbml_content
        assert 'Ref:' in dbml_content  # Relationships

        # Verify 2024 parser compliance
        assert 'Table users [headercolor:' in dbml_content  # Proper spacing
        assert '"text []"' in dbml_content  # Quoted array types

        # Phase 6: Quality validation
        silent_detector = SilentFailureDetector()
        silent_failures = silent_detector.detect_silent_failures(schema_data, processed_schema)

        # Phase 7: Quality metrics
        metrics_calculator = SixSigmaMetrics()
        quality_metrics = metrics_calculator.calculate_metrics(
            schema_data, processed_schema, silent_failures
        )

        # Verify quality metrics are reasonable
        assert quality_metrics['sigma_level'] >= 0
        assert quality_metrics['semantic_preservation'] >= 0.5  # Should preserve most structure
        assert quality_metrics['compatibility_score']['overall_score'] >= 0.7

        # Verify no critical silent failures
        critical_failures = [f for f in silent_failures if f['severity'] == 'CRITICAL']
        assert len(critical_failures) == 0

    def test_write_and_validate_output(self):
        """Test writing DBML output and validating it."""

        # Run conversion pipeline
        with open(self.sample_sql_path, 'r') as f:
            sql_content = f.read()

        cleaner = SQLCleaner()
        cleaned_sql = cleaner.clean_dump(sql_content)

        parser = SQLParser()
        schema_data = parser.parse_sql_dump(cleaned_sql)

        type_mapper = TypeMapper()
        transformed_schema = type_mapper.transform_types(schema_data)

        constraint_handler = ConstraintHandler()
        processed_schema = constraint_handler.process_constraints(transformed_schema)

        generator = DBMLGenerator()
        dbml_content = generator.generate(processed_schema)

        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.dbml', delete=False) as f:
            f.write(dbml_content)
            temp_path = f.name

        try:
            # Verify file was written and is readable
            assert os.path.exists(temp_path)
            with open(temp_path, 'r') as f:
                written_content = f.read()

            assert written_content == dbml_content

            # Basic syntax validation
            assert written_content.count('{') == written_content.count('}')  # Balanced braces
            assert 'Table ' in written_content
            assert 'Ref:' in written_content

        finally:
            # Clean up
            os.unlink(temp_path)

    def test_error_recovery(self):
        """Test error recovery with malformed SQL."""

        malformed_sql = """
        CREATE TABLE broken (
            id integer,
            -- Missing closing parenthesis and semicolon

        SELECT invalid_syntax FROM nowhere;
        """

        # Should not crash even with malformed input
        cleaner = SQLCleaner()
        cleaned_sql = cleaner.clean_dump(malformed_sql)

        parser = SQLParser()
        schema_data = parser.parse_sql_dump(cleaned_sql)

        # Should have parsing errors but continue
        assert 'parsing_errors' in schema_data
        # Should still attempt to process what it can
        assert 'tables' in schema_data

    def test_empty_schema_handling(self):
        """Test handling of empty or minimal schemas."""

        minimal_sql = """
        SET client_encoding = 'UTF8';
        -- Just comments and configuration
        """

        cleaner = SQLCleaner()
        cleaned_sql = cleaner.clean_dump(minimal_sql)

        parser = SQLParser()
        schema_data = parser.parse_sql_dump(cleaned_sql)

        # Should handle empty schema gracefully
        assert schema_data['tables'] == []
        assert schema_data['relationships'] == []

        # Should still generate valid DBML
        generator = DBMLGenerator()
        dbml_content = generator.generate(schema_data)

        assert 'Project postgresql_schema' in dbml_content
        # Should not crash on empty tables


class TestEndToEnd:
    """End-to-end tests simulating real usage."""

    def test_realistic_schema_conversion(self):
        """Test with a more realistic schema."""

        realistic_sql = """
        CREATE TABLE companies (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            industry VARCHAR(100),
            founded_year INTEGER CHECK (founded_year > 1800),
            employee_count INTEGER DEFAULT 0,
            website_url TEXT,
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            department VARCHAR(50),
            salary NUMERIC(10,2),
            hire_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT true,
            skills TEXT[],
            FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
        );

        CREATE INDEX idx_employees_company ON employees(company_id);
        CREATE INDEX idx_employees_email ON employees(email);
        CREATE INDEX idx_employees_department ON employees(department) WHERE department IS NOT NULL;
        """

        # Run complete pipeline
        cleaner = SQLCleaner()
        cleaned_sql = cleaner.clean_dump(realistic_sql)

        parser = SQLParser()
        schema_data = parser.parse_sql_dump(cleaned_sql)

        type_mapper = TypeMapper()
        transformed_schema = type_mapper.transform_types(schema_data)

        constraint_handler = ConstraintHandler()
        processed_schema = constraint_handler.process_constraints(transformed_schema)

        generator = DBMLGenerator()
        dbml_content = generator.generate(processed_schema)

        # Verify key elements are present
        assert 'Table companies' in dbml_content
        assert 'Table employees' in dbml_content
        assert 'serial' not in dbml_content.lower()  # Should be converted to integer
        assert '"text []"' in dbml_content  # Array type should be quoted
        assert 'Ref:' in dbml_content  # Foreign key relationship

        # Verify quality
        silent_detector = SilentFailureDetector()
        silent_failures = silent_detector.detect_silent_failures(schema_data, processed_schema)

        metrics_calculator = SixSigmaMetrics()
        quality_metrics = metrics_calculator.calculate_metrics(
            schema_data, processed_schema, silent_failures
        )

        # Should achieve reasonable quality
        assert quality_metrics['sigma_level'] >= 3.0
        assert quality_metrics['semantic_preservation'] >= 0.7
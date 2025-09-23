"""
Tests for type transformations and constraint handling.
"""

import pytest
from src.transformer.type_mapper import TypeMapper
from src.transformer.constraint_handler import ConstraintHandler


class TestTypeMapper:
    """Test type mapping functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mapper = TypeMapper()

    def test_basic_type_mapping(self):
        """Test basic PostgreSQL to DBML type mapping."""
        schema = {
            'tables': [{
                'table_name': 'users',
                'columns': [
                    {'column_name': 'id', 'data_type': 'integer'},
                    {'column_name': 'name', 'data_type': 'varchar'},
                    {'column_name': 'active', 'data_type': 'boolean'},
                ]
            }]
        }

        result = self.mapper.transform_types(schema)

        columns = result['tables'][0]['columns']
        assert columns[0]['data_type'] == 'integer'
        assert columns[1]['data_type'] == 'varchar'
        assert columns[2]['data_type'] == 'boolean'

    def test_array_type_transformation(self):
        """Test array type transformations."""
        schema = {
            'tables': [{
                'table_name': 'test',
                'columns': [
                    {'column_name': 'tags', 'data_type': 'text[]'},
                    {'column_name': 'scores', 'data_type': 'integer[]'},
                ]
            }]
        }

        result = self.mapper.transform_types(schema)

        columns = result['tables'][0]['columns']
        assert columns[0]['data_type'] == '"text []"'
        assert columns[1]['data_type'] == '"integer []"'

        # Check transformations were logged
        assert len(result['type_transformations']) == 2

    def test_unsupported_type_fallback(self):
        """Test fallback to text for unsupported types."""
        schema = {
            'tables': [{
                'table_name': 'test',
                'columns': [
                    {'column_name': 'ip', 'data_type': 'inet'},
                    {'column_name': 'location', 'data_type': 'point'},
                    {'column_name': 'search', 'data_type': 'tsvector'},
                ]
            }]
        }

        result = self.mapper.transform_types(schema)

        columns = result['tables'][0]['columns']
        assert columns[0]['data_type'] == 'text'
        assert columns[1]['data_type'] == 'text'
        assert columns[2]['data_type'] == 'text'

        # Check warnings were generated
        assert len(result['type_warnings']) >= 3

    def test_type_with_parameters(self):
        """Test types with parameters are preserved."""
        schema = {
            'tables': [{
                'table_name': 'test',
                'columns': [
                    {'column_name': 'name', 'data_type': 'varchar(255)'},
                    {'column_name': 'price', 'data_type': 'numeric(10,2)'},
                ]
            }]
        }

        result = self.mapper.transform_types(schema)

        columns = result['tables'][0]['columns']
        assert columns[0]['data_type'] == 'varchar(255)'
        assert columns[1]['data_type'] == 'numeric(10,2)'

    def test_transformation_report(self):
        """Test transformation report generation."""
        schema = {
            'tables': [{
                'table_name': 'test',
                'columns': [
                    {'column_name': 'tags', 'data_type': 'text[]'},
                    {'column_name': 'ip', 'data_type': 'inet'},
                ]
            }]
        }

        self.mapper.transform_types(schema)
        report = self.mapper.get_transformation_report()

        assert report['total_transformations'] == 2
        assert report['total_warnings'] >= 2
        assert 'by_reason' in report


class TestConstraintHandler:
    """Test constraint handling functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = ConstraintHandler()

    def test_keep_supported_constraints(self):
        """Test that supported constraints are kept."""
        schema = {
            'tables': [{
                'table_name': 'users',
                'constraints': [
                    {
                        'constraint_name': 'pk_users',
                        'constraint_type': 'p',
                        'columns': ['id']
                    },
                    {
                        'constraint_name': 'uq_email',
                        'constraint_type': 'u',
                        'columns': ['email']
                    },
                    {
                        'constraint_name': 'fk_department',
                        'constraint_type': 'f',
                        'columns': ['dept_id'],
                        'referenced_table': 'departments',
                        'referenced_columns': ['id']
                    }
                ]
            }]
        }

        result = self.handler.process_constraints(schema)

        constraints = result['tables'][0]['constraints']
        assert len(constraints) == 3  # All should be kept

    def test_drop_check_constraints(self):
        """Test that CHECK constraints are dropped."""
        schema = {
            'tables': [{
                'table_name': 'users',
                'constraints': [
                    {
                        'constraint_name': 'pk_users',
                        'constraint_type': 'p',
                        'columns': ['id']
                    },
                    {
                        'constraint_name': 'chk_age',
                        'constraint_type': 'c',
                        'check_expression': 'age >= 0'
                    }
                ]
            }]
        }

        result = self.handler.process_constraints(schema)

        constraints = result['tables'][0]['constraints']
        assert len(constraints) == 1  # Only primary key should remain

        # Check that drop was logged
        assert len(result['dropped_constraints']) == 1
        assert result['dropped_constraints'][0]['constraint_type'] == 'CHECK'

    def test_remove_deferrable_from_foreign_keys(self):
        """Test removal of DEFERRABLE from foreign key constraints."""
        schema = {
            'tables': [{
                'table_name': 'orders',
                'constraints': [
                    {
                        'constraint_name': 'fk_customer',
                        'constraint_type': 'f',
                        'definition': 'FOREIGN KEY (customer_id) REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED',
                        'columns': ['customer_id'],
                        'referenced_table': 'customers',
                        'referenced_columns': ['id']
                    }
                ]
            }]
        }

        result = self.handler.process_constraints(schema)

        constraint = result['tables'][0]['constraints'][0]
        assert 'DEFERRABLE' not in constraint['definition']

        # Check that modification was logged
        assert len(result['modified_constraints']) == 1

    def test_constraint_report(self):
        """Test constraint processing report."""
        schema = {
            'tables': [{
                'table_name': 'test',
                'constraints': [
                    {
                        'constraint_name': 'pk_test',
                        'constraint_type': 'p',
                        'columns': ['id']
                    },
                    {
                        'constraint_name': 'chk_positive',
                        'constraint_type': 'c',
                        'check_expression': 'value > 0'
                    }
                ]
            }]
        }

        self.handler.process_constraints(schema)
        report = self.handler.get_constraint_report()

        assert report['total_dropped'] == 1
        assert report['total_modified'] == 0
        assert 'CHECK' in report['dropped_by_type']
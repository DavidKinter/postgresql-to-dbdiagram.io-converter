"""
Tests for silent failure detection.
"""

import pytest
from src.quality.silent_failure_detector import SilentFailureDetector


class TestSilentFailureDetector:
    """Test silent failure detection functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.detector = SilentFailureDetector()

    def test_detect_missing_tables(self):
        """Test detection of missing tables."""
        original = {
            'tables': [
                {'table_name': 'users'},
                {'table_name': 'orders'},
                {'table_name': 'products'}
            ]
        }

        converted = {
            'tables': [
                {'table_name': 'users'},
                {'table_name': 'orders'}
            ],
            'warnings': []
        }

        failures = self.detector.detect_silent_failures(original, converted)

        # Should detect missing 'products' table
        missing_table_failures = [f for f in failures if f['type'] == 'MISSING_TABLE']
        assert len(missing_table_failures) == 1
        assert missing_table_failures[0]['table'] == 'products'

    def test_detect_missing_relationships(self):
        """Test detection of missing relationships."""
        original = {
            'tables': [],
            'relationships': [
                {
                    'source_table': 'orders',
                    'target_table': 'customers'
                },
                {
                    'source_table': 'orders',
                    'target_table': 'products'
                }
            ]
        }

        converted = {
            'tables': [],
            'relationships': [
                {
                    'source_table': 'orders',
                    'target_table': 'customers'
                }
            ],
            'warnings': []
        }

        failures = self.detector.detect_silent_failures(original, converted)

        # Should detect missing orders->products relationship
        missing_rel_failures = [f for f in failures if f['type'] == 'MISSING_RELATIONSHIP']
        assert len(missing_rel_failures) == 1
        assert missing_rel_failures[0]['source_table'] == 'orders'
        assert missing_rel_failures[0]['target_table'] == 'products'

    def test_detect_missing_constraints(self):
        """Test detection of missing constraints."""
        original = {
            'tables': [{
                'table_name': 'users',
                'constraints': [
                    {'constraint_type': 'p'},  # Primary key
                    {'constraint_type': 'c'},  # Check constraint
                    {'constraint_type': 'c'},  # Another check constraint
                ]
            }]
        }

        converted = {
            'tables': [{
                'table_name': 'users',
                'constraints': [
                    {'constraint_type': 'p'}  # Only primary key remains
                ]
            }],
            'warnings': []
        }

        failures = self.detector.detect_silent_failures(original, converted)

        # Should detect missing check constraints
        missing_constraint_failures = [f for f in failures if f['type'] == 'MISSING_CONSTRAINT']
        assert len(missing_constraint_failures) == 1
        assert missing_constraint_failures[0]['constraint_type'] == 'check'
        assert missing_constraint_failures[0]['lost_count'] == 2

    def test_detect_invisible_cascade_actions(self):
        """Test detection of invisible CASCADE actions."""
        converted = {
            'relationships': [
                {
                    'source_table': 'orders',
                    'target_table': 'customers',
                    'on_delete_action': 'CASCADE',
                    'on_update_action': None
                },
                {
                    'source_table': 'order_items',
                    'target_table': 'orders',
                    'on_delete_action': None,
                    'on_update_action': 'CASCADE'
                }
            ]
        }

        failures = self.detector.detect_silent_failures({}, converted)

        # Should detect invisible CASCADE actions
        cascade_failures = [f for f in failures if f['type'] == 'INVISIBLE_CASCADE']
        assert len(cascade_failures) == 2

        # Check specific CASCADE types
        delete_cascade = next(f for f in cascade_failures if 'DELETE' in f['cascade_types'])
        update_cascade = next(f for f in cascade_failures if 'UPDATE' in f['cascade_types'])

        assert delete_cascade['relationship']['source_table'] == 'orders'
        assert update_cascade['relationship']['source_table'] == 'order_items'

    def test_detect_missing_columns(self):
        """Test detection of missing columns."""
        original = {
            'tables': [{
                'table_name': 'users',
                'columns': [
                    {'column_name': 'id'},
                    {'column_name': 'name'},
                    {'column_name': 'email'},
                    {'column_name': 'password_hash'}
                ]
            }]
        }

        converted = {
            'tables': [{
                'table_name': 'users',
                'columns': [
                    {'column_name': 'id'},
                    {'column_name': 'name'},
                    {'column_name': 'email'}
                    # password_hash missing
                ]
            }],
            'warnings': []
        }

        failures = self.detector.detect_silent_failures(original, converted)

        # Should detect missing password_hash column
        missing_column_failures = [f for f in failures if f['type'] == 'MISSING_COLUMN']
        assert len(missing_column_failures) == 1
        assert missing_column_failures[0]['column'] == 'password_hash'

    def test_no_silent_failures_when_documented(self):
        """Test that no silent failures are detected when losses are documented."""
        original = {
            'tables': [
                {'table_name': 'users'},
                {'table_name': 'orders'}
            ]
        }

        converted = {
            'tables': [
                {'table_name': 'users'}
            ],
            'warnings': [
                {'message': 'Table orders was dropped due to incompatibility'}
            ]
        }

        failures = self.detector.detect_silent_failures(original, converted)

        # Should not detect missing 'orders' table because it's documented in warnings
        missing_table_failures = [f for f in failures if f['type'] == 'MISSING_TABLE']
        assert len(missing_table_failures) == 0

    def test_get_failure_report(self):
        """Test failure report generation."""
        original = {
            'tables': [
                {'table_name': 'users'},
                {'table_name': 'orders'}
            ],
            'relationships': [
                {'source_table': 'orders', 'target_table': 'users'}
            ]
        }

        converted = {
            'tables': [
                {'table_name': 'users'}
            ],
            'relationships': [],
            'warnings': []
        }

        self.detector.detect_silent_failures(original, converted)
        report = self.detector.get_failure_report()

        assert report['total_failures'] == 2  # Missing table and relationship
        assert 'MISSING_TABLE' in report['by_type']
        assert 'MISSING_RELATIONSHIP' in report['by_type']
        assert report['critical_count'] >= 1  # Missing table is critical
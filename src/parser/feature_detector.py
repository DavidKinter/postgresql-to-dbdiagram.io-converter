"""
PostgreSQL feature detection for incompatibility analysis.
"""

from typing import Dict, List, Any, Set
import re

class FeatureDetector:
    """Detect PostgreSQL-specific features that may cause DBML compatibility issues."""

    def __init__(self):
        self.detected_features = []
        self.compatibility_issues = []

    def detect_features(self, schema_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect PostgreSQL features and assess DBML compatibility.
        """

        self.detected_features = []
        self.compatibility_issues = []

        # Detect features in tables and columns
        self._detect_table_features(schema_data.get('tables', []))

        # Detect constraint features
        self._detect_constraint_features(schema_data.get('constraints', []))

        # Detect index features
        self._detect_index_features(schema_data.get('indexes', []))

        # Detect relationship features
        self._detect_relationship_features(schema_data.get('relationships', []))

        return {
            'detected_features': self.detected_features,
            'compatibility_issues': self.compatibility_issues,
            'feature_summary': self._summarize_features(),
            'critical_issues': self._get_critical_issues()
        }

    def _detect_table_features(self, tables: List[Dict]):
        """Detect features in table and column definitions."""
        for table in tables:
            table_name = table['table_name']

            # Detect column features
            for column in table.get('columns', []):
                self._detect_column_features(column, table_name)

            # Detect table-level features
            self._detect_table_level_features(table)

    def _detect_column_features(self, column: Dict, table_name: str):
        """Detect features in individual columns."""
        column_name = column['column_name']
        data_type = column['data_type']

        # Array types (Critical incompatibility)
        if column.get('is_array_type', False):
            self._add_feature({
                'feature_type': 'ARRAY_TYPE',
                'severity': 'CRITICAL',
                'location': f"{table_name}.{column_name}",
                'data_type': data_type,
                'description': f"Array type '{data_type}' requires special handling",
                'impact': 'Syntax errors during import without quoting',
                'workaround': 'Quote array type syntax'
            })

        # PostgreSQL-specific types
        base_type = column.get('base_type', '').lower()
        self._detect_postgresql_specific_types(base_type, table_name, column_name, data_type)

        # UUID functions in defaults
        default_value = column.get('default_value', '')
        if default_value and 'gen_random_uuid()' in default_value:
            self._add_feature({
                'feature_type': 'UUID_FUNCTION',
                'severity': 'HIGH',
                'location': f"{table_name}.{column_name}",
                'data_type': data_type,
                'default_value': default_value,
                'description': 'UUID generation function requires transformation',
                'impact': 'Parser error on import',
                'workaround': 'Convert to backtick notation'
            })

        # Negative defaults
        if default_value and default_value.startswith('-') and default_value[1:].isdigit():
            self._add_feature({
                'feature_type': 'NEGATIVE_DEFAULT',
                'severity': 'MEDIUM',
                'location': f"{table_name}.{column_name}",
                'data_type': data_type,
                'default_value': default_value,
                'description': 'Negative default value requires quoting',
                'impact': 'Parser error on import',
                'workaround': 'Quote negative values'
            })

    def _detect_postgresql_specific_types(self, base_type: str, table_name: str,
                                        column_name: str, full_type: str):
        """Detect PostgreSQL-specific data types."""

        # Geometric types
        geometric_types = {'point', 'line', 'lseg', 'box', 'path', 'polygon', 'circle'}
        if base_type in geometric_types:
            self._add_feature({
                'feature_type': 'GEOMETRIC_TYPE',
                'severity': 'HIGH',
                'location': f"{table_name}.{column_name}",
                'data_type': full_type,
                'description': f"Geometric type '{base_type}' not supported in DBML",
                'impact': 'Type lost, converted to text',
                'workaround': 'Convert to text with semantic loss'
            })

        # Network types
        network_types = {'inet', 'cidr', 'macaddr', 'macaddr8'}
        if base_type in network_types:
            self._add_feature({
                'feature_type': 'NETWORK_TYPE',
                'severity': 'HIGH',
                'location': f"{table_name}.{column_name}",
                'data_type': full_type,
                'description': f"Network type '{base_type}' not supported in DBML",
                'impact': 'Type semantics lost, converted to text',
                'workaround': 'Convert to text with validation loss'
            })

        # Range types
        range_types = {'int4range', 'int8range', 'numrange', 'tsrange', 'tstzrange', 'daterange'}
        if base_type in range_types:
            self._add_feature({
                'feature_type': 'RANGE_TYPE',
                'severity': 'HIGH',
                'location': f"{table_name}.{column_name}",
                'data_type': full_type,
                'description': f"Range type '{base_type}' not supported in DBML",
                'impact': 'Range semantics lost, converted to text',
                'workaround': 'Convert to text with range validation loss'
            })

        # Multirange types (PostgreSQL 14+)
        multirange_types = {'int4multirange', 'int8multirange', 'nummultirange',
                           'tsmultirange', 'tstzmultirange', 'datemultirange'}
        if base_type in multirange_types:
            self._add_feature({
                'feature_type': 'MULTIRANGE_TYPE',
                'severity': 'CRITICAL',
                'location': f"{table_name}.{column_name}",
                'data_type': full_type,
                'description': f"Multirange type '{base_type}' (PostgreSQL 14+) not supported",
                'impact': 'Modern feature completely lost',
                'workaround': 'Convert to text with complete semantic loss'
            })

        # Text search types
        text_search_types = {'tsvector', 'tsquery'}
        if base_type in text_search_types:
            self._add_feature({
                'feature_type': 'TEXT_SEARCH_TYPE',
                'severity': 'HIGH',
                'location': f"{table_name}.{column_name}",
                'data_type': full_type,
                'description': f"Text search type '{base_type}' not supported",
                'impact': 'Full-text search capabilities lost',
                'workaround': 'Convert to text with search optimization loss'
            })

        # Other PostgreSQL types
        other_pg_types = {'xml', 'money', 'bytea', 'bit', 'varbit'}
        if base_type in other_pg_types:
            self._add_feature({
                'feature_type': 'POSTGRESQL_SPECIFIC_TYPE',
                'severity': 'MEDIUM',
                'location': f"{table_name}.{column_name}",
                'data_type': full_type,
                'description': f"PostgreSQL-specific type '{base_type}' not supported",
                'impact': 'Type-specific features lost',
                'workaround': 'Convert to compatible type with feature loss'
            })

    def _detect_table_level_features(self, table: Dict):
        """Detect table-level PostgreSQL features."""
        table_name = table['table_name']
        original_statement = table.get('original_statement', '')

        # Table inheritance
        if 'INHERITS' in original_statement.upper():
            self._add_feature({
                'feature_type': 'TABLE_INHERITANCE',
                'severity': 'CRITICAL',
                'location': table_name,
                'description': 'Table inheritance not supported in DBML',
                'impact': 'Inheritance hierarchy completely lost',
                'workaround': 'Flatten hierarchy or use composition'
            })

        # Partitioning
        partition_keywords = ['PARTITION BY', 'PARTITION OF']
        if any(keyword in original_statement.upper() for keyword in partition_keywords):
            self._add_feature({
                'feature_type': 'TABLE_PARTITIONING',
                'severity': 'CRITICAL',
                'location': table_name,
                'description': 'Table partitioning not supported in DBML',
                'impact': 'Partitioning strategy completely lost',
                'workaround': 'Represent as separate tables'
            })

    def _detect_constraint_features(self, constraints: List[Dict]):
        """Detect constraint-related features."""
        for constraint in constraints:
            constraint_type = constraint.get('constraint_type')
            table_name = constraint.get('table_name', 'unknown')

            # CHECK constraints
            if constraint_type == 'c':
                self._add_feature({
                    'feature_type': 'CHECK_CONSTRAINT',
                    'severity': 'HIGH',
                    'location': f"{table_name}.{constraint.get('constraint_name', 'unnamed')}",
                    'description': 'CHECK constraint not supported in DBML',
                    'impact': 'Business logic validation lost',
                    'workaround': 'Implement in application logic'
                })

            # Deferrable constraints
            definition = constraint.get('definition', '')
            if 'DEFERRABLE' in definition.upper():
                self._add_feature({
                    'feature_type': 'DEFERRABLE_CONSTRAINT',
                    'severity': 'HIGH',
                    'location': f"{table_name}.{constraint.get('constraint_name', 'unnamed')}",
                    'description': 'Deferrable constraint not supported in DBML',
                    'impact': 'Transaction-safe constraint behavior lost',
                    'workaround': 'Handle in application transaction logic'
                })

            # Exclude constraints
            if definition.upper().startswith('EXCLUDE'):
                self._add_feature({
                    'feature_type': 'EXCLUDE_CONSTRAINT',
                    'severity': 'CRITICAL',
                    'location': f"{table_name}.{constraint.get('constraint_name', 'unnamed')}",
                    'description': 'EXCLUDE constraint not supported in DBML',
                    'impact': 'Spatial/range exclusion logic completely lost',
                    'workaround': 'Implement complex application logic'
                })

    def _detect_index_features(self, indexes: List[Dict]):
        """Detect index-related features."""
        for index in indexes:
            table_name = index.get('table_name', 'unknown')
            index_name = index.get('index_name', 'unnamed')
            definition = index.get('definition', '')

            # Partial indexes
            if 'WHERE' in definition.upper():
                self._add_feature({
                    'feature_type': 'PARTIAL_INDEX',
                    'severity': 'HIGH',
                    'location': f"{table_name}.{index_name}",
                    'description': 'Partial index not supported in DBML',
                    'impact': 'Conditional indexing optimization lost',
                    'workaround': 'Create full index with performance impact'
                })

            # Expression indexes
            if re.search(r'\([^)]*\([^)]*\)[^)]*\)', definition):
                self._add_feature({
                    'feature_type': 'EXPRESSION_INDEX',
                    'severity': 'HIGH',
                    'location': f"{table_name}.{index_name}",
                    'description': 'Expression/functional index not supported',
                    'impact': 'Function-based indexing optimization lost',
                    'workaround': 'Create simple column indexes'
                })

            # Concurrent creation
            if 'CONCURRENTLY' in definition.upper():
                self._add_feature({
                    'feature_type': 'CONCURRENT_INDEX',
                    'severity': 'MEDIUM',
                    'location': f"{table_name}.{index_name}",
                    'description': 'CONCURRENTLY option not represented in DBML',
                    'impact': 'Index creation strategy information lost',
                    'workaround': 'Index created normally'
                })

            # Operator classes
            if re.search(r'\w+_ops', definition):
                self._add_feature({
                    'feature_type': 'OPERATOR_CLASS',
                    'severity': 'HIGH',
                    'location': f"{table_name}.{index_name}",
                    'description': 'Index operator class not supported',
                    'impact': 'Index optimization strategy lost',
                    'workaround': 'Use default operator class'
                })

    def _detect_relationship_features(self, relationships: List[Dict]):
        """Detect relationship-related features."""
        for rel in relationships:
            source_table = rel.get('source_table', 'unknown')
            target_table = rel.get('target_table', 'unknown')

            # CASCADE actions
            on_delete = rel.get('on_delete_action')
            on_update = rel.get('on_update_action')

            if on_delete == 'CASCADE' or on_update == 'CASCADE':
                self._add_feature({
                    'feature_type': 'CASCADE_ACTION',
                    'severity': 'MEDIUM',
                    'location': f"{source_table} -> {target_table}",
                    'description': 'CASCADE action exists but not visualized',
                    'impact': 'Critical referential integrity behavior hidden',
                    'workaround': 'Document CASCADE behavior separately'
                })

            # Composite foreign keys
            if rel.get('is_composite', False):
                self._add_feature({
                    'feature_type': 'COMPOSITE_FOREIGN_KEY',
                    'severity': 'HIGH',
                    'location': f"{source_table} -> {target_table}",
                    'description': 'Composite foreign key may cause parsing issues',
                    'impact': 'Potential parser errors or relationship loss',
                    'workaround': 'Verify relationship imports correctly'
                })

    def _add_feature(self, feature: Dict):
        """Add detected feature to list."""
        self.detected_features.append(feature)

        # Add to compatibility issues if severity is HIGH or CRITICAL
        if feature['severity'] in ['HIGH', 'CRITICAL']:
            self.compatibility_issues.append(feature)

    def _summarize_features(self) -> Dict[str, Any]:
        """Summarize detected features by type and severity."""
        summary = {
            'total_features': len(self.detected_features),
            'by_severity': {},
            'by_type': {}
        }

        for feature in self.detected_features:
            severity = feature['severity']
            feature_type = feature['feature_type']

            # Count by severity
            if severity not in summary['by_severity']:
                summary['by_severity'][severity] = 0
            summary['by_severity'][severity] += 1

            # Count by type
            if feature_type not in summary['by_type']:
                summary['by_type'][feature_type] = 0
            summary['by_type'][feature_type] += 1

        return summary

    def _get_critical_issues(self) -> List[Dict]:
        """Get only critical compatibility issues."""
        return [f for f in self.compatibility_issues if f['severity'] == 'CRITICAL']
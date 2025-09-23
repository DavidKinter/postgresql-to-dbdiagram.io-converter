"""
Silent failure detection to prevent data loss without warnings.

Addresses Lines 88, 121: "Silent partial import failures" are critical quality issues.
"""

from typing import Dict, List, Any

class SilentFailureDetector:
    """Detect cases where data is lost without explicit warnings."""

    def __init__(self):
        self.detected_failures = []

    def detect_silent_failures(self, original_schema: Dict, converted_schema: Dict) -> List[Dict]:
        """
        Detect all forms of silent data loss.

        Critical detection areas from analysis:
        - Line 88: "Silent failures common"
        - Line 121: "Some tables process while others skip silently"
        - Line 96: "CASCADE actions not visualized"
        """

        failures = []

        # Detect missing tables
        failures.extend(self._detect_missing_tables(original_schema, converted_schema))

        # Detect missing relationships
        failures.extend(self._detect_missing_relationships(original_schema, converted_schema))

        # Detect missing constraints without warnings
        failures.extend(self._detect_missing_constraints(original_schema, converted_schema))

        # Detect invisible CASCADE actions (Line 96)
        failures.extend(self._detect_invisible_cascade_actions(converted_schema))

        # Detect missing columns
        failures.extend(self._detect_missing_columns(original_schema, converted_schema))

        self.detected_failures = failures

        return failures

    def _detect_missing_tables(self, original: Dict, converted: Dict) -> List[Dict]:
        """Detect tables that disappeared without explanation."""

        failures = []

        original_tables = {t['table_name'] for t in original.get('tables', [])}
        converted_tables = {t['table_name'] for t in converted.get('tables', [])}

        missing_tables = original_tables - converted_tables

        for table_name in missing_tables:
            # Check if there's an explicit warning about this table
            table_warned = any(
                table_name in warning.get('message', '')
                for warning in converted.get('warnings', [])
            )

            if not table_warned:
                failures.append({
                    'type': 'MISSING_TABLE',
                    'severity': 'CRITICAL',
                    'description': f"Table '{table_name}' missing from output without warning",
                    'table': table_name,
                    'impact': 'Complete table lost',
                    'detection_method': 'table_count_comparison'
                })

        return failures

    def _detect_missing_relationships(self, original: Dict, converted: Dict) -> List[Dict]:
        """Detect foreign key relationships that vanished."""

        failures = []

        # Count relationships by table pairs
        def get_relationship_signature(rel):
            return (rel.get('source_table', ''), rel.get('target_table', ''))

        original_rels = {
            get_relationship_signature(rel): rel
            for rel in original.get('relationships', [])
        }

        converted_rels = {
            get_relationship_signature(rel): rel
            for rel in converted.get('relationships', [])
        }

        missing_rel_signatures = set(original_rels.keys()) - set(converted_rels.keys())

        for rel_sig in missing_rel_signatures:
            if rel_sig[0] and rel_sig[1]:  # Valid relationship signature
                original_rel = original_rels[rel_sig]

                # Check if this relationship loss was explicitly documented
                rel_warned = any(
                    rel_sig[0] in warning.get('message', '') and
                    rel_sig[1] in warning.get('message', '')
                    for warning in converted.get('warnings', [])
                )

                if not rel_warned:
                    failures.append({
                        'type': 'MISSING_RELATIONSHIP',
                        'severity': 'HIGH',
                        'description': f"Relationship {rel_sig[0]} -> {rel_sig[1]} lost without warning",
                        'source_table': rel_sig[0],
                        'target_table': rel_sig[1],
                        'original_relationship': original_rel,
                        'impact': 'Foreign key constraint lost',
                        'detection_method': 'relationship_signature_comparison'
                    })

        return failures

    def _detect_missing_constraints(self, original: Dict, converted: Dict) -> List[Dict]:
        """Detect constraints that disappeared without documentation."""

        failures = []

        # Count constraints by table
        def count_constraints_by_table(schema):
            counts = {}
            for table in schema.get('tables', []):
                table_name = table['table_name']
                counts[table_name] = {
                    'check': 0,
                    'unique': 0,
                    'foreign_key': 0,
                    'primary_key': 0
                }

                for constraint in table.get('constraints', []):
                    constraint_type = constraint.get('constraint_type', '').lower()
                    if constraint_type == 'c':
                        counts[table_name]['check'] += 1
                    elif constraint_type == 'u':
                        counts[table_name]['unique'] += 1
                    elif constraint_type == 'f':
                        counts[table_name]['foreign_key'] += 1
                    elif constraint_type == 'p':
                        counts[table_name]['primary_key'] += 1

            return counts

        original_counts = count_constraints_by_table(original)
        converted_counts = count_constraints_by_table(converted)

        for table_name in original_counts:
            if table_name not in converted_counts:
                continue

            orig_counts = original_counts[table_name]
            conv_counts = converted_counts[table_name]

            for constraint_type, orig_count in orig_counts.items():
                conv_count = conv_counts.get(constraint_type, 0)
                lost_count = orig_count - conv_count

                if lost_count > 0:
                    # Check if constraint loss was documented
                    constraint_warned = any(
                        constraint_type.upper() in warning.get('message', '').upper() and
                        table_name in warning.get('message', '')
                        for warning in converted.get('warnings', [])
                    )

                    if not constraint_warned:
                        failures.append({
                            'type': 'MISSING_CONSTRAINT',
                            'severity': 'HIGH' if constraint_type == 'check' else 'MEDIUM',
                            'description': f"Lost {lost_count} {constraint_type} constraint(s) on table '{table_name}' without warning",
                            'table': table_name,
                            'constraint_type': constraint_type,
                            'lost_count': lost_count,
                            'impact': f"Data integrity rules lost",
                            'detection_method': 'constraint_count_comparison'
                        })

        return failures

    def _detect_invisible_cascade_actions(self, converted: Dict) -> List[Dict]:
        """
        Detect CASCADE actions that exist but aren't visualized (Line 96).

        "While ON DELETE CASCADE imports successfully, no visual indication
        appears in diagrams. Critical referential integrity information becomes invisible."
        """

        failures = []

        for rel in converted.get('relationships', []):
            # Check if relationship has CASCADE actions
            has_cascade_delete = rel.get('on_delete_action') == 'CASCADE'
            has_cascade_update = rel.get('on_update_action') == 'CASCADE'

            if has_cascade_delete or has_cascade_update:
                # Check if CASCADE action is documented in the relationship
                # For DBML, CASCADE actions should be in comments or notes
                rel_definition = str(rel)
                cascade_documented = 'CASCADE' in rel_definition.upper()

                # Also check if it's in generated DBML comments
                cascade_in_comments = False
                if 'definition' in rel:
                    cascade_in_comments = 'CASCADE' in rel.get('definition', '').upper()

                if not (cascade_documented or cascade_in_comments):
                    cascade_types = []
                    if has_cascade_delete:
                        cascade_types.append('DELETE')
                    if has_cascade_update:
                        cascade_types.append('UPDATE')

                    failures.append({
                        'type': 'INVISIBLE_CASCADE',
                        'severity': 'MEDIUM',
                        'description': f"CASCADE {'/'.join(cascade_types)} action on {rel.get('source_table', 'unknown')} -> {rel.get('target_table', 'unknown')} not visible in diagram",
                        'relationship': rel,
                        'cascade_types': cascade_types,
                        'impact': 'Critical referential integrity behavior hidden',
                        'detection_method': 'cascade_action_visibility_check'
                    })

        return failures

    def _detect_missing_columns(self, original: Dict, converted: Dict) -> List[Dict]:
        """Detect columns that disappeared from tables."""

        failures = []

        # Create column mapping
        def get_table_columns(schema):
            table_columns = {}
            for table in schema.get('tables', []):
                table_name = table['table_name']
                table_columns[table_name] = {
                    col['column_name'] for col in table.get('columns', [])
                }
            return table_columns

        original_columns = get_table_columns(original)
        converted_columns = get_table_columns(converted)

        for table_name in original_columns:
            if table_name not in converted_columns:
                continue

            missing_columns = original_columns[table_name] - converted_columns[table_name]

            for column_name in missing_columns:
                # Check if column loss was documented
                column_warned = any(
                    column_name in warning.get('message', '') and
                    table_name in warning.get('message', '')
                    for warning in converted.get('warnings', [])
                )

                if not column_warned:
                    failures.append({
                        'type': 'MISSING_COLUMN',
                        'severity': 'HIGH',
                        'description': f"Column '{table_name}.{column_name}' missing from output without warning",
                        'table': table_name,
                        'column': column_name,
                        'impact': 'Column data definition lost',
                        'detection_method': 'column_inventory_comparison'
                    })

        return failures

    def get_failure_report(self) -> Dict:
        """Generate comprehensive silent failure report."""

        # Group by type and severity
        by_type = {}
        by_severity = {}

        for failure in self.detected_failures:
            failure_type = failure['type']
            severity = failure['severity']

            if failure_type not in by_type:
                by_type[failure_type] = []
            by_type[failure_type].append(failure)

            if severity not in by_severity:
                by_severity[severity] = []
            by_severity[severity].append(failure)

        return {
            'total_failures': len(self.detected_failures),
            'by_type': by_type,
            'by_severity': by_severity,
            'critical_count': len(by_severity.get('CRITICAL', [])),
            'high_count': len(by_severity.get('HIGH', [])),
            'medium_count': len(by_severity.get('MEDIUM', [])),
            'all_failures': self.detected_failures
        }
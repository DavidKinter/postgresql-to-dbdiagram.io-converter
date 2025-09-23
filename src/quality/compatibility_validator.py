"""
Compatibility validation for dbdiagram.io parser requirements.
"""

from typing import Dict, List, Any, Optional
import re

class CompatibilityValidator:
    """Validate schema compatibility with dbdiagram.io requirements."""

    def __init__(self):
        self.validation_issues = []
        self.compatibility_score = 0.0

    def validate_compatibility(self, schema: Dict, dbml_content: str) -> Dict[str, Any]:
        """
        Validate complete compatibility with dbdiagram.io.

        Checks schema structure and generated DBML for known issues.
        """

        self.validation_issues = []

        # Validate schema structure
        self._validate_schema_structure(schema)

        # Validate DBML content
        self._validate_dbml_content(dbml_content)

        # Calculate compatibility score
        self.compatibility_score = self._calculate_compatibility_score()

        return {
            'is_compatible': len(self.validation_issues) == 0,
            'compatibility_score': self.compatibility_score,
            'total_issues': len(self.validation_issues),
            'issues': self.validation_issues,
            'recommendations': self._get_recommendations()
        }

    def _validate_schema_structure(self, schema: Dict):
        """Validate schema structure for compatibility issues."""

        tables = schema.get('tables', [])

        for table in tables:
            self._validate_table_compatibility(table)

        relationships = schema.get('relationships', [])
        for relationship in relationships:
            self._validate_relationship_compatibility(relationship)

    def _validate_table_compatibility(self, table: Dict):
        """Validate individual table for compatibility issues."""

        table_name = table['table_name']
        columns = table.get('columns', [])

        # Check for problematic table names
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            self._add_issue(
                'INVALID_TABLE_NAME',
                'HIGH',
                f"Table name '{table_name}' contains invalid characters",
                f"Table names should contain only letters, numbers, and underscores"
            )

        # Check for tables without primary keys
        has_primary_key = any(
            col.get('is_primary_key', False) for col in columns
        ) or any(
            constraint.get('constraint_type') == 'p'
            for constraint in table.get('constraints', [])
        )

        if not has_primary_key:
            self._add_issue(
                'NO_PRIMARY_KEY',
                'MEDIUM',
                f"Table '{table_name}' has no primary key",
                f"Primary keys improve diagram clarity and relationship definition"
            )

        # Validate columns
        for column in columns:
            self._validate_column_compatibility(column, table_name)

    def _validate_column_compatibility(self, column: Dict, table_name: str):
        """Validate individual column for compatibility issues."""

        column_name = column['column_name']
        data_type = column['data_type']

        # Check for problematic column names
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            self._add_issue(
                'INVALID_COLUMN_NAME',
                'HIGH',
                f"Column '{table_name}.{column_name}' contains invalid characters",
                f"Column names should contain only letters, numbers, and underscores"
            )

        # Check for reserved keywords
        reserved_keywords = {
            'table', 'ref', 'enum', 'project', 'note', 'indexes',
            'and', 'or', 'not', 'null', 'true', 'false'
        }

        if column_name.lower() in reserved_keywords:
            self._add_issue(
                'RESERVED_KEYWORD',
                'MEDIUM',
                f"Column '{table_name}.{column_name}' uses reserved keyword",
                f"Consider renaming to avoid potential parsing issues"
            )

        # Check for complex data types
        if self._is_complex_type(data_type):
            self._add_issue(
                'COMPLEX_DATA_TYPE',
                'MEDIUM',
                f"Column '{table_name}.{column_name}' uses complex type '{data_type}'",
                f"Complex types may not display correctly in diagrams"
            )

        # Check for extremely long column names
        if len(column_name) > 50:
            self._add_issue(
                'LONG_COLUMN_NAME',
                'LOW',
                f"Column '{table_name}.{column_name}' has very long name",
                f"Long names may be truncated in diagram display"
            )

    def _validate_relationship_compatibility(self, relationship: Dict):
        """Validate relationship for compatibility issues."""

        source_table = relationship.get('source_table', 'unknown')
        target_table = relationship.get('target_table', 'unknown')
        source_columns = relationship.get('source_columns', [])
        target_columns = relationship.get('target_columns', [])

        # Check for composite relationships (known issues)
        if len(source_columns) > 1:
            self._add_issue(
                'COMPOSITE_RELATIONSHIP',
                'HIGH',
                f"Composite relationship {source_table} -> {target_table}",
                f"Composite foreign keys may cause parser errors (GitHub Issue #142)"
            )

        # Check for self-referencing relationships
        if source_table == target_table:
            self._add_issue(
                'SELF_REFERENCING',
                'MEDIUM',
                f"Self-referencing relationship on table '{source_table}'",
                f"May cause diagram layout issues or confusion"
            )

        # Check for CASCADE actions
        if relationship.get('on_delete_action') == 'CASCADE' or relationship.get('on_update_action') == 'CASCADE':
            self._add_issue(
                'CASCADE_ACTION_INVISIBLE',
                'MEDIUM',
                f"CASCADE action on relationship {source_table} -> {target_table}",
                f"CASCADE actions exist but are not visualized in diagrams"
            )

    def _validate_dbml_content(self, dbml_content: str):
        """Validate generated DBML content for syntax issues."""

        lines = dbml_content.split('\n')

        for line_num, line in enumerate(lines, 1):
            line_stripped = line.strip()

            if not line_stripped or line_stripped.startswith('//'):
                continue

            # Check for 2024 parser syntax issues
            self._validate_dbml_line_syntax(line_stripped, line_num)

    def _validate_dbml_line_syntax(self, line: str, line_num: int):
        """Validate individual DBML line for syntax compatibility."""

        # Table settings spacing (2024 requirement)
        if re.match(r'Table\s+\w+\[', line):
            self._add_issue(
                'TABLE_SETTINGS_SPACING',
                'HIGH',
                f"Line {line_num}: Missing space before table settings bracket",
                f"2024 parser requires 'Table name [settings]' not 'Table name[settings]'"
            )

        # Unquoted negative defaults
        if re.search(r'default:\s*-\d+(?!\w)', line):
            self._add_issue(
                'UNQUOTED_NEGATIVE_DEFAULT',
                'HIGH',
                f"Line {line_num}: Unquoted negative default value",
                f"Negative defaults must be quoted: default: '-1'"
            )

        # Unquoted array types
        if re.search(r'\w+\[\](?!\s)', line) and '"' not in line:
            self._add_issue(
                'UNQUOTED_ARRAY_TYPE',
                'HIGH',
                f"Line {line_num}: Unquoted array type",
                f"Array types must be quoted: \"type []\""
            )

    def _is_complex_type(self, data_type: str) -> bool:
        """Check if data type is considered complex for DBML."""

        complex_patterns = [
            r'.*\[\].*',  # Arrays
            r'.*\(.*,.*\).*',  # Types with multiple parameters
            r'^".*"$',  # Quoted types (often indicate complexity)
        ]

        return any(re.match(pattern, data_type) for pattern in complex_patterns)

    def _calculate_compatibility_score(self) -> float:
        """Calculate overall compatibility score."""

        if not self.validation_issues:
            return 1.0

        # Weight issues by severity
        total_weight = 0
        for issue in self.validation_issues:
            severity = issue['severity']
            if severity == 'HIGH':
                total_weight += 3
            elif severity == 'MEDIUM':
                total_weight += 2
            else:  # LOW
                total_weight += 1

        # Calculate score (max penalty of 0.5 for any single issue type)
        max_possible_weight = len(self.validation_issues) * 3
        score = max(0.0, 1.0 - (total_weight / max(max_possible_weight, 10)))

        return score

    def _add_issue(self, issue_type: str, severity: str, description: str, recommendation: str):
        """Add compatibility issue."""

        issue = {
            'issue_type': issue_type,
            'severity': severity,
            'description': description,
            'recommendation': recommendation
        }

        self.validation_issues.append(issue)

    def _get_recommendations(self) -> List[str]:
        """Get high-level recommendations for improving compatibility."""

        recommendations = []

        # Group issues by type
        issue_types = {}
        for issue in self.validation_issues:
            issue_type = issue['issue_type']
            if issue_type not in issue_types:
                issue_types[issue_type] = 0
            issue_types[issue_type] += 1

        # Generate recommendations based on common issues
        if 'COMPOSITE_RELATIONSHIP' in issue_types:
            recommendations.append(
                "Consider breaking composite foreign keys into separate relationships "
                "to avoid parser errors"
            )

        if 'COMPLEX_DATA_TYPE' in issue_types:
            recommendations.append(
                "Simplify complex data types for better diagram display compatibility"
            )

        if 'NO_PRIMARY_KEY' in issue_types:
            recommendations.append(
                "Add primary keys to tables to improve diagram clarity and relationship definition"
            )

        if 'CASCADE_ACTION_INVISIBLE' in issue_types:
            recommendations.append(
                "Document CASCADE actions separately as they are not visible in diagrams"
            )

        if not recommendations:
            recommendations.append("Schema appears highly compatible with dbdiagram.io")

        return recommendations

    def get_compatibility_summary(self) -> str:
        """Get human-readable compatibility summary."""

        if self.compatibility_score >= 0.95:
            status = "✅ EXCELLENT"
        elif self.compatibility_score >= 0.85:
            status = "✅ GOOD"
        elif self.compatibility_score >= 0.70:
            status = "⚠️  ACCEPTABLE"
        else:
            status = "❌ POOR"

        summary = f"{status} - Compatibility Score: {self.compatibility_score:.1%}"

        if self.validation_issues:
            high_issues = sum(1 for issue in self.validation_issues if issue['severity'] == 'HIGH')
            if high_issues > 0:
                summary += f" ({high_issues} high-priority issues to address)"

        return summary
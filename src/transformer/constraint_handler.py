"""
Constraint processing and handling for DBML compatibility.

Addresses incompatibilities with CHECK, EXCLUDE, and DEFERRABLE constraints.
"""

from typing import Dict, List, Any, Optional

class ConstraintHandler:
    """Handle constraint transformations and removals for DBML compatibility."""

    def __init__(self):
        self.dropped_constraints = []
        self.modified_constraints = []
        self.warnings_generated = []

    def process_constraints(self, schema: Dict) -> Dict:
        """
        Process all constraints in schema for DBML compatibility.

        Removes unsupported constraints and documents all changes.
        """

        processed_schema = schema.copy()

        # Process table-level constraints
        for table in processed_schema.get('tables', []):
            table['constraints'] = self._process_table_constraints(
                table.get('constraints', []),
                table['table_name']
            )

        # Process standalone constraints
        if 'constraints' in processed_schema:
            processed_schema['constraints'] = self._process_standalone_constraints(
                processed_schema['constraints']
            )

        # Apply PRIMARY KEY and UNIQUE constraints to column definitions
        self._apply_constraints_to_columns(processed_schema)

        # Add metadata about constraint processing
        processed_schema['dropped_constraints'] = self.dropped_constraints
        processed_schema['modified_constraints'] = self.modified_constraints
        processed_schema['constraint_warnings'] = self.warnings_generated

        return processed_schema

    def _process_table_constraints(self, constraints: List[Dict], table_name: str) -> List[Dict]:
        """Process constraints for a specific table."""
        processed_constraints = []

        for constraint in constraints:
            result = self._process_single_constraint(constraint, table_name)
            if result is not None:
                processed_constraints.append(result)

        return processed_constraints

    def _process_standalone_constraints(self, constraints: List[Dict]) -> List[Dict]:
        """Process standalone constraints (from ALTER TABLE statements)."""
        processed_constraints = []

        for constraint in constraints:
            table_name = constraint.get('table_name', 'unknown')
            result = self._process_single_constraint(constraint, table_name)
            if result is not None:
                processed_constraints.append(result)

        return processed_constraints

    def _process_single_constraint(self, constraint: Dict, table_name: str) -> Optional[Dict]:
        """
        Process a single constraint based on DBML compatibility.

        Returns None if constraint should be dropped, modified constraint otherwise.
        """

        constraint_type = constraint.get('constraint_type')
        constraint_name = constraint.get('constraint_name', 'unnamed')

        # Primary Key constraints - keep as-is (supported)
        if constraint_type == 'p':
            return constraint

        # Unique constraints - keep as-is (supported)
        elif constraint_type == 'u':
            return constraint

        # Foreign Key constraints - process for compatibility
        elif constraint_type == 'f':
            return self._process_foreign_key_constraint(constraint, table_name)

        # CHECK constraints - drop with warning (not supported - Line 38)
        elif constraint_type == 'c':
            return self._drop_check_constraint(constraint, table_name)

        # Unknown constraint type - drop with warning
        else:
            return self._drop_unknown_constraint(constraint, table_name)

    def _process_foreign_key_constraint(self, constraint: Dict, table_name: str) -> Dict:
        """Process foreign key constraint for DBML compatibility."""

        # Check for unsupported features
        definition = constraint.get('definition', '')
        constraint_name = constraint.get('constraint_name', 'unnamed')

        # Check for DEFERRABLE (not supported in DBML)
        if 'DEFERRABLE' in definition.upper():
            self._log_constraint_modification(
                constraint_name,
                table_name,
                'FOREIGN_KEY_DEFERRABLE',
                'Removed DEFERRABLE option - not supported in DBML'
            )

            # Remove DEFERRABLE from definition
            constraint = constraint.copy()
            constraint['definition'] = self._remove_deferrable_from_definition(definition)

        # Check for composite foreign keys (potential issues)
        source_columns = constraint.get('columns', [])
        if len(source_columns) > 1:
            self._generate_warning(
                f"Composite foreign key '{constraint_name}' on table '{table_name}' - "
                f"verify import compatibility (known parser issues exist)"
            )

        return constraint

    def _drop_check_constraint(self, constraint: Dict, table_name: str) -> None:
        """
        Drop CHECK constraint with comprehensive logging.

        Based on Line 38: CHECK constraints cause parser errors and are on roadmap since 2020.
        """

        constraint_name = constraint.get('constraint_name', 'unnamed')
        check_expression = constraint.get('check_expression', constraint.get('definition', ''))

        self._log_constraint_drop(
            constraint_name,
            table_name,
            'CHECK',
            'CHECK constraints not supported in DBML (parser errors)',
            {
                'check_expression': check_expression,
                'impact': 'Business logic validation lost',
                'workaround': 'Implement validation in application layer',
                'reference': 'Incompatibility Analysis Line 38'
            }
        )

        return None

    def _drop_unknown_constraint(self, constraint: Dict, table_name: str) -> None:
        """Drop unknown constraint type with logging."""

        constraint_name = constraint.get('constraint_name', 'unnamed')
        definition = constraint.get('definition', '')

        # Try to identify the constraint type from definition
        definition_upper = definition.upper()

        if definition_upper.startswith('EXCLUDE'):
            constraint_type = 'EXCLUDE'
            reason = 'EXCLUDE constraints not supported in DBML'
            impact = 'Spatial/range exclusion logic completely lost'
        else:
            constraint_type = 'UNKNOWN'
            reason = 'Unknown constraint type not supported in DBML'
            impact = 'Constraint logic lost'

        self._log_constraint_drop(
            constraint_name,
            table_name,
            constraint_type,
            reason,
            {
                'definition': definition,
                'impact': impact,
                'workaround': 'Implement logic in application layer'
            }
        )

        return None

    def _remove_deferrable_from_definition(self, definition: str) -> str:
        """Remove DEFERRABLE clause from constraint definition."""
        import re

        # Remove DEFERRABLE INITIALLY DEFERRED/IMMEDIATE
        cleaned = re.sub(
            r'\s+DEFERRABLE(\s+INITIALLY\s+(DEFERRED|IMMEDIATE))?\s*',
            ' ',
            definition,
            flags=re.IGNORECASE
        )

        return cleaned.strip()

    def _log_constraint_drop(self, constraint_name: str, table_name: str,
                           constraint_type: str, reason: str, metadata: Dict):
        """Log constraint drop with comprehensive information."""

        drop_record = {
            'constraint_name': constraint_name,
            'table_name': table_name,
            'constraint_type': constraint_type,
            'reason': reason,
            'metadata': metadata,
            'action': 'DROPPED'
        }

        self.dropped_constraints.append(drop_record)

        # Generate user-facing warning
        warning_message = (
            f"Dropped {constraint_type} constraint '{constraint_name}' "
            f"on table '{table_name}': {reason}"
        )
        self._generate_warning(warning_message)

    def _log_constraint_modification(self, constraint_name: str, table_name: str,
                                   modification_type: str, description: str):
        """Log constraint modification."""

        modification_record = {
            'constraint_name': constraint_name,
            'table_name': table_name,
            'modification_type': modification_type,
            'description': description,
            'action': 'MODIFIED'
        }

        self.modified_constraints.append(modification_record)

        # Generate user-facing warning
        warning_message = (
            f"Modified constraint '{constraint_name}' on table '{table_name}': {description}"
        )
        self._generate_warning(warning_message)

    def _generate_warning(self, message: str):
        """Generate a warning message."""
        self.warnings_generated.append({
            'type': 'CONSTRAINT_WARNING',
            'message': message
        })

    def get_constraint_report(self) -> Dict[str, Any]:
        """Generate comprehensive constraint processing report."""

        # Group dropped constraints by type
        dropped_by_type = {}
        for dropped in self.dropped_constraints:
            constraint_type = dropped['constraint_type']
            if constraint_type not in dropped_by_type:
                dropped_by_type[constraint_type] = []
            dropped_by_type[constraint_type].append(dropped)

        # Group modifications by type
        modified_by_type = {}
        for modified in self.modified_constraints:
            mod_type = modified['modification_type']
            if mod_type not in modified_by_type:
                modified_by_type[mod_type] = []
            modified_by_type[mod_type].append(modified)

        return {
            'total_dropped': len(self.dropped_constraints),
            'total_modified': len(self.modified_constraints),
            'total_warnings': len(self.warnings_generated),
            'dropped_by_type': dropped_by_type,
            'modified_by_type': modified_by_type,
            'all_dropped': self.dropped_constraints,
            'all_modified': self.modified_constraints,
            'warnings': self.warnings_generated
        }

    def _apply_constraints_to_columns(self, schema: Dict):
        """Apply PRIMARY KEY and UNIQUE constraints to column definitions."""
        # Collect all constraints from both table-level and standalone
        all_constraints = []

        # Get table-level constraints
        for table in schema.get('tables', []):
            table_constraints = table.get('constraints', [])
            for constraint in table_constraints:
                constraint['source_table'] = table['table_name']
                all_constraints.append(constraint)

        # Get standalone constraints
        standalone_constraints = schema.get('constraints', [])
        all_constraints.extend(standalone_constraints)

        # Apply constraints to columns
        for constraint in all_constraints:
            constraint_type = constraint.get('constraint_type')
            table_name = constraint.get('table_name') or constraint.get('source_table')
            columns = constraint.get('columns', [])

            if constraint_type == 'p' and columns:  # PRIMARY KEY
                self._mark_columns_as_primary_key(schema, table_name, columns)
            elif constraint_type == 'u' and columns:  # UNIQUE
                self._mark_columns_as_unique(schema, table_name, columns)

    def _mark_columns_as_primary_key(self, schema: Dict, table_name: str, column_names: List[str]):
        """Mark columns as primary key."""
        for table in schema.get('tables', []):
            if table['table_name'] == table_name:
                for column in table.get('columns', []):
                    if column['column_name'] in column_names:
                        column['is_primary_key'] = True
                        # Primary keys are implicitly NOT NULL and UNIQUE
                        column['is_nullable'] = False
                        if len(column_names) == 1:
                            column['is_unique'] = True

    def _mark_columns_as_unique(self, schema: Dict, table_name: str, column_names: List[str]):
        """Mark columns as unique."""
        for table in schema.get('tables', []):
            if table['table_name'] == table_name:
                for column in table.get('columns', []):
                    if column['column_name'] in column_names and len(column_names) == 1:
                        # Only mark single-column unique constraints
                        column['is_unique'] = True
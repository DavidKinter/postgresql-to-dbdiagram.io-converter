"""
Relationship building and validation for DBML output.
"""

from typing import Dict, List, Any, Optional, Set, Tuple

class RelationshipBuilder:
    """Build and validate relationships for DBML output."""

    def __init__(self):
        self.built_relationships = []
        self.skipped_relationships = []
        self.relationship_warnings = []

    def build_relationships(self, relationships: List[Dict], tables: List[Dict]) -> List[Dict]:
        """
        Build valid DBML relationships from schema relationships.

        Validates and filters relationships for DBML compatibility.
        """

        self.built_relationships = []
        self.skipped_relationships = []
        self.relationship_warnings = []

        # Create table and column lookup
        table_lookup = self._create_table_lookup(tables)

        for relationship in relationships:
            built_rel = self._build_single_relationship(relationship, table_lookup)
            if built_rel:
                self.built_relationships.append(built_rel)

        # Detect and handle duplicate relationships
        self._deduplicate_relationships()

        return self.built_relationships

    def _create_table_lookup(self, tables: List[Dict]) -> Dict[str, Dict]:
        """Create lookup for table and column validation."""

        lookup = {}

        for table in tables:
            table_name = table['table_name']
            columns = {col['column_name']: col for col in table.get('columns', [])}

            lookup[table_name] = {
                'table': table,
                'columns': columns
            }

        return lookup

    def _build_single_relationship(self, relationship: Dict,
                                 table_lookup: Dict[str, Dict]) -> Optional[Dict]:
        """Build and validate a single relationship."""

        source_table = relationship['source_table']
        source_columns = relationship['source_columns']
        target_table = relationship['target_table']
        target_columns = relationship['target_columns']

        # Validate that tables exist
        if source_table not in table_lookup:
            self._skip_relationship(
                relationship,
                f"Source table '{source_table}' not found in schema",
                "MISSING_SOURCE_TABLE"
            )
            return None

        if target_table not in table_lookup:
            self._skip_relationship(
                relationship,
                f"Target table '{target_table}' not found in schema",
                "MISSING_TARGET_TABLE"
            )
            return None

        # Validate that columns exist
        source_table_info = table_lookup[source_table]
        target_table_info = table_lookup[target_table]

        missing_source_columns = [
            col for col in source_columns
            if col not in source_table_info['columns']
        ]

        missing_target_columns = [
            col for col in target_columns
            if col not in target_table_info['columns']
        ]

        if missing_source_columns:
            self._skip_relationship(
                relationship,
                f"Source columns not found: {missing_source_columns}",
                "MISSING_SOURCE_COLUMNS"
            )
            return None

        if missing_target_columns:
            self._skip_relationship(
                relationship,
                f"Target columns not found: {missing_target_columns}",
                "MISSING_TARGET_COLUMNS"
            )
            return None

        # Validate column count match
        if len(source_columns) != len(target_columns):
            self._skip_relationship(
                relationship,
                f"Column count mismatch: {len(source_columns)} source, {len(target_columns)} target",
                "COLUMN_COUNT_MISMATCH"
            )
            return None

        # Determine relationship type
        rel_type = self._determine_relationship_type(
            relationship,
            source_table_info,
            target_table_info
        )

        # Build the relationship
        built_rel = {
            'source_table': source_table,
            'source_columns': source_columns,
            'target_table': target_table,
            'target_columns': target_columns,
            'relationship_type': rel_type,
            'constraint_name': relationship.get('constraint_name'),
            'on_delete_action': relationship.get('on_delete_action'),
            'on_update_action': relationship.get('on_update_action'),
            'is_composite': len(source_columns) > 1,
            'is_self_referencing': source_table == target_table
        }

        # Add warnings for problematic relationships
        if built_rel['is_composite']:
            self._generate_warning(
                f"Composite relationship {source_table} -> {target_table} may cause parser issues"
            )

        if built_rel['is_self_referencing']:
            self._generate_warning(
                f"Self-referencing relationship on {source_table} - verify diagram clarity"
            )

        return built_rel

    def _determine_relationship_type(self, relationship: Dict,
                                   source_table_info: Dict,
                                   target_table_info: Dict) -> str:
        """
        Determine the relationship type (one-to-one, one-to-many, many-to-one).

        This is a simplified determination - in practice, you'd need to
        analyze uniqueness constraints more thoroughly.
        """

        source_columns = relationship['source_columns']
        target_columns = relationship['target_columns']

        # Check if source columns form a unique constraint
        source_is_unique = self._columns_are_unique(source_columns, source_table_info)

        # Check if target columns form a unique constraint (should be for FK)
        target_is_unique = self._columns_are_unique(target_columns, target_table_info)

        # Determine relationship type
        if source_is_unique and target_is_unique:
            return 'one-to-one'
        elif target_is_unique:
            return 'many-to-one'
        else:
            # This shouldn't happen with valid foreign keys, but handle gracefully
            self._generate_warning(
                f"Relationship {relationship['source_table']} -> {relationship['target_table']} "
                f"references non-unique columns - unusual foreign key"
            )
            return 'many-to-one'

    def _columns_are_unique(self, columns: List[str], table_info: Dict) -> bool:
        """Check if columns form a unique constraint."""

        table = table_info['table']

        # Check for primary key
        pk_columns = set()
        for constraint in table.get('constraints', []):
            if constraint.get('constraint_type') == 'p':
                pk_columns.update(constraint.get('columns', []))

        if set(columns) == pk_columns:
            return True

        # Check for unique constraints
        for constraint in table.get('constraints', []):
            if constraint.get('constraint_type') == 'u':
                unique_columns = set(constraint.get('columns', []))
                if set(columns) == unique_columns:
                    return True

        # Check column-level unique constraints
        if len(columns) == 1:
            column_name = columns[0]
            column_info = table_info['columns'].get(column_name, {})
            if column_info.get('is_unique', False) or column_info.get('is_primary_key', False):
                return True

        return False

    def _deduplicate_relationships(self):
        """Remove duplicate relationships."""

        seen_relationships = set()
        deduplicated = []

        for rel in self.built_relationships:
            # Create a signature for the relationship
            signature = self._create_relationship_signature(rel)

            if signature not in seen_relationships:
                seen_relationships.add(signature)
                deduplicated.append(rel)
            else:
                self._generate_warning(
                    f"Duplicate relationship {rel['source_table']} -> {rel['target_table']} removed"
                )

        self.built_relationships = deduplicated

    def _create_relationship_signature(self, relationship: Dict) -> Tuple:
        """Create a unique signature for a relationship."""

        return (
            relationship['source_table'],
            tuple(relationship['source_columns']),
            relationship['target_table'],
            tuple(relationship['target_columns'])
        )

    def _skip_relationship(self, relationship: Dict, reason: str, skip_type: str):
        """Log a skipped relationship."""

        skip_record = {
            'relationship': relationship,
            'reason': reason,
            'skip_type': skip_type,
            'source_table': relationship.get('source_table', 'unknown'),
            'target_table': relationship.get('target_table', 'unknown')
        }

        self.skipped_relationships.append(skip_record)

        # Generate warning
        self._generate_warning(
            f"Skipped relationship {relationship.get('source_table', 'unknown')} -> "
            f"{relationship.get('target_table', 'unknown')}: {reason}"
        )

    def _generate_warning(self, message: str):
        """Generate a warning message."""
        self.relationship_warnings.append({
            'type': 'RELATIONSHIP_WARNING',
            'message': message
        })

    def get_relationship_report(self) -> Dict[str, Any]:
        """Get comprehensive relationship building report."""

        # Group skipped relationships by type
        skipped_by_type = {}
        for skipped in self.skipped_relationships:
            skip_type = skipped['skip_type']
            if skip_type not in skipped_by_type:
                skipped_by_type[skip_type] = []
            skipped_by_type[skip_type].append(skipped)

        # Count relationship types
        relationship_type_counts = {}
        for rel in self.built_relationships:
            rel_type = rel['relationship_type']
            if rel_type not in relationship_type_counts:
                relationship_type_counts[rel_type] = 0
            relationship_type_counts[rel_type] += 1

        return {
            'total_built': len(self.built_relationships),
            'total_skipped': len(self.skipped_relationships),
            'total_warnings': len(self.relationship_warnings),
            'relationship_type_counts': relationship_type_counts,
            'skipped_by_type': skipped_by_type,
            'composite_relationships': sum(1 for r in self.built_relationships if r['is_composite']),
            'self_referencing_relationships': sum(1 for r in self.built_relationships if r['is_self_referencing']),
            'cascade_relationships': sum(1 for r in self.built_relationships
                                       if r.get('on_delete_action') == 'CASCADE' or r.get('on_update_action') == 'CASCADE'),
            'all_skipped': self.skipped_relationships,
            'warnings': self.relationship_warnings
        }
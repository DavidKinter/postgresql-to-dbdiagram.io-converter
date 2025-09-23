"""
Schema extraction utilities for organizing parsed SQL components.
"""

from typing import Dict, List, Any, Optional

class SchemaExtractor:
    """Extract and organize schema components from parsed SQL."""

    def __init__(self):
        pass

    def extract_schema_info(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract comprehensive schema information from parsed SQL data.
        """

        schema_info = {
            'tables': self._organize_tables(parsed_data),
            'relationships': self._organize_relationships(parsed_data),
            'indexes': self._organize_indexes(parsed_data),
            'sequences': self._organize_sequences(parsed_data),
            'enums': self._organize_enums(parsed_data),
            'constraints': self._organize_constraints(parsed_data),
            'statistics': self._calculate_statistics(parsed_data),
            'parsing_errors': parsed_data.get('parsing_errors', [])
        }

        return schema_info

    def _organize_tables(self, parsed_data: Dict[str, Any]) -> List[Dict]:
        """Organize table information with enhanced metadata."""
        tables = []

        for table in parsed_data.get('tables', []):
            organized_table = {
                'table_name': table['table_name'],
                'columns': self._organize_columns(table['columns']),
                'constraints': table.get('constraints', []),
                'primary_key_columns': self._extract_primary_key_columns(table),
                'foreign_keys': self._extract_foreign_keys(table, parsed_data),
                'column_count': len(table['columns']),
                'has_primary_key': self._has_primary_key(table),
                'original_statement': table.get('original_statement', '')
            }

            tables.append(organized_table)

        return tables

    def _organize_columns(self, columns: List[Dict]) -> List[Dict]:
        """Organize column information with enhanced metadata."""
        organized_columns = []

        for column in columns:
            organized_column = {
                'column_name': column['column_name'],
                'data_type': column['data_type'],
                'is_nullable': column.get('is_nullable', True),
                'default_value': column.get('default_value'),
                'constraints': column.get('constraints', []),
                'is_primary_key': 'PRIMARY KEY' in column.get('constraints', []),
                'is_unique': 'UNIQUE' in column.get('constraints', []),
                'is_array_type': self._is_array_type(column['data_type']),
                'base_type': self._extract_base_type(column['data_type']),
                'type_parameters': self._extract_type_parameters(column['data_type']),
                'original_definition': column.get('original_definition', '')
            }

            organized_columns.append(organized_column)

        return organized_columns

    def _organize_relationships(self, parsed_data: Dict[str, Any]) -> List[Dict]:
        """Organize relationship information."""
        relationships = []

        for rel in parsed_data.get('relationships', []):
            organized_rel = {
                'source_table': rel['source_table'],
                'source_columns': rel['source_columns'],
                'target_table': rel['target_table'],
                'target_columns': rel['target_columns'],
                'relationship_type': self._determine_relationship_type(rel),
                'constraint_name': rel.get('constraint_name'),
                'on_delete_action': rel.get('on_delete_action'),
                'on_update_action': rel.get('on_update_action'),
                'is_composite': len(rel['source_columns']) > 1,
                'is_self_referencing': rel['source_table'] == rel['target_table']
            }

            relationships.append(organized_rel)

        return relationships

    def _organize_indexes(self, parsed_data: Dict[str, Any]) -> List[Dict]:
        """Organize index information."""
        indexes = []

        for index in parsed_data.get('indexes', []):
            organized_index = {
                'index_name': index['index_name'],
                'table_name': index['table_name'],
                'columns': index['columns'],
                'is_unique': index.get('is_unique', False),
                'is_composite': len(index['columns']) > 1,
                'column_count': len(index['columns']),
                'definition': index.get('definition', '')
            }

            indexes.append(organized_index)

        return indexes

    def _organize_sequences(self, parsed_data: Dict[str, Any]) -> List[Dict]:
        """Organize sequence information."""
        sequences = []

        for sequence in parsed_data.get('sequences', []):
            organized_sequence = {
                'sequence_name': sequence['sequence_name'],
                'definition': sequence.get('definition', '')
            }

            sequences.append(organized_sequence)

        return sequences

    def _organize_enums(self, parsed_data: Dict[str, Any]) -> List[Dict]:
        """Organize enum information."""
        enums = []

        for enum in parsed_data.get('enums', []):
            organized_enum = {
                'enum_name': enum['enum_name'],
                'values': enum['values'],
                'definition': enum.get('definition', '')
            }
            enums.append(organized_enum)

        return enums

    def _organize_constraints(self, parsed_data: Dict[str, Any]) -> List[Dict]:
        """Organize constraint information."""
        constraints = []

        # Collect constraints from tables
        for table in parsed_data.get('tables', []):
            for constraint in table.get('constraints', []):
                constraints.append(constraint)

        # Add standalone constraints
        for constraint in parsed_data.get('constraints', []):
            constraints.append(constraint)

        return constraints

    def _calculate_statistics(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate schema statistics."""
        tables = parsed_data.get('tables', [])
        relationships = parsed_data.get('relationships', [])
        indexes = parsed_data.get('indexes', [])

        total_columns = sum(len(table['columns']) for table in tables)
        total_constraints = sum(len(table.get('constraints', [])) for table in tables)

        array_columns = 0
        nullable_columns = 0
        primary_key_columns = 0

        for table in tables:
            for column in table['columns']:
                if self._is_array_type(column['data_type']):
                    array_columns += 1
                if column.get('is_nullable', True):
                    nullable_columns += 1
                if 'PRIMARY KEY' in column.get('constraints', []):
                    primary_key_columns += 1

        return {
            'total_tables': len(tables),
            'total_columns': total_columns,
            'total_relationships': len(relationships),
            'total_indexes': len(indexes),
            'total_constraints': total_constraints,
            'array_columns': array_columns,
            'nullable_columns': nullable_columns,
            'primary_key_columns': primary_key_columns,
            'composite_relationships': sum(1 for r in relationships if len(r['source_columns']) > 1),
            'self_referencing_relationships': sum(1 for r in relationships if r['source_table'] == r['target_table'])
        }

    def _extract_primary_key_columns(self, table: Dict) -> List[str]:
        """Extract primary key column names."""
        pk_columns = []

        # Check column-level constraints
        for column in table['columns']:
            if 'PRIMARY KEY' in column.get('constraints', []):
                pk_columns.append(column['column_name'])

        # Check table-level constraints
        for constraint in table.get('constraints', []):
            if constraint.get('constraint_type') == 'p':
                pk_columns.extend(constraint.get('columns', []))

        return list(set(pk_columns))

    def _extract_foreign_keys(self, table: Dict, parsed_data: Dict) -> List[Dict]:
        """Extract foreign key information for a table."""
        foreign_keys = []

        # Get relationships where this table is the source
        for rel in parsed_data.get('relationships', []):
            if rel['source_table'] == table['table_name']:
                foreign_keys.append({
                    'source_columns': rel['source_columns'],
                    'target_table': rel['target_table'],
                    'target_columns': rel['target_columns'],
                    'constraint_name': rel.get('constraint_name'),
                    'on_delete_action': rel.get('on_delete_action'),
                    'on_update_action': rel.get('on_update_action')
                })

        return foreign_keys

    def _has_primary_key(self, table: Dict) -> bool:
        """Check if table has a primary key."""
        # Check column-level primary keys
        for column in table['columns']:
            if 'PRIMARY KEY' in column.get('constraints', []):
                return True

        # Check table-level primary keys
        for constraint in table.get('constraints', []):
            if constraint.get('constraint_type') == 'p':
                return True

        return False

    def _is_array_type(self, data_type: str) -> bool:
        """Check if data type is an array."""
        return '[]' in data_type

    def _extract_base_type(self, data_type: str) -> str:
        """Extract base type from complex type definition."""
        # Remove array notation
        base_type = data_type.replace('[]', '')

        # Remove parameters
        if '(' in base_type:
            base_type = base_type.split('(')[0]

        return base_type.strip()

    def _extract_type_parameters(self, data_type: str) -> Optional[str]:
        """Extract type parameters (e.g., length, precision)."""
        if '(' in data_type and ')' in data_type:
            start = data_type.find('(')
            end = data_type.find(')')
            return data_type[start+1:end]
        return None

    def _determine_relationship_type(self, relationship: Dict) -> str:
        """Determine relationship type based on constraints."""
        # This is a simplified determination
        # In practice, you'd need to analyze the uniqueness of foreign key columns
        return relationship.get('relationship_type', 'many-to-one')
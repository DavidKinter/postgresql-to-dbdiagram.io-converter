"""
PostgreSQL to DBML type mapping with comprehensive transformation rules.

Based on incompatibility analysis Section 1 (Data Type Incompatibilities).
"""

from typing import Dict, List, Any, Tuple
import re

class TypeMapper:
    """Handle PostgreSQL to DBML type transformations."""

    def __init__(self):
        self.transformations_applied = []
        self.warnings_generated = []

        # Core type mapping matrix - Updated based on official dbdiagram.io support
        self.type_map = {
            # Fully supported native PostgreSQL types
            'integer': 'int4',          # Use native PG types
            'int': 'int4',
            'int4': 'int4',
            'bigint': 'int8',
            'int8': 'int8',
            'smallint': 'int2',
            'int2': 'int2',
            'boolean': 'bool',
            'bool': 'bool',
            'text': 'text',
            'varchar': 'varchar',
            'char': 'bpchar',
            'bpchar': 'bpchar',
            'date': 'date',
            'timestamp': 'timestamp',
            'timestamptz': 'timestamptz',
            'time': 'time',
            'timetz': 'timetz',
            'interval': 'interval',
            'json': 'json',
            'jsonb': 'jsonb',          # Native support confirmed!
            'uuid': 'uuid',

            # Numeric types with native support
            'numeric': 'numeric',
            'decimal': 'numeric',      # DECIMAL maps to numeric in PG
            'real': 'float4',
            'float4': 'float4',
            'float8': 'float8',
            'float': 'float8',
            'double': 'float8',

            # Serial types map to their underlying int types
            'serial': 'int4',
            'bigserial': 'int8',
            'smallserial': 'int2',

            # Network, geometric, and binary types - Native support!
            'inet': 'inet',
            'cidr': 'cidr',
            'macaddr': 'macaddr',
            'macaddr8': 'macaddr8',
            'point': 'point',
            'line': 'line',
            'lseg': 'lseg',
            'box': 'box',
            'path': 'path',
            'polygon': 'polygon',
            'circle': 'circle',
            'bytea': 'bytea',
            'money': 'money',

            # Range types - Native support!
            'int4range': 'int4range',
            'int8range': 'int8range',
            'numrange': 'numrange',
            'tsrange': 'tsrange',
            'tstzrange': 'tstzrange',
            'daterange': 'daterange',

            # Text search and other advanced types
            'tsvector': 'tsvector',
            'tsquery': 'tsquery',
            'xml': 'xml',
            'pg_lsn': 'pg_lsn',

            # Bit types
            'bit': 'bit',
            'varbit': 'varbit',

            # Only newer multirange types fall back to text
            'int4multirange': 'text',
            'int8multirange': 'text',
            'nummultirange': 'text',
            'tsmultirange': 'text',
            'datemultirange': 'text',

            # PostgreSQL extension types that cause semantic loss
            'hstore': 'text',
            'ltree': 'text',
            'cube': 'text',
            'isbn': 'text',
            'issn': 'text'
        }

    def transform_types(self, schema: Dict, decisions: Dict = None) -> Dict:
        """Transform all types in schema according to decisions."""

        if decisions is None:
            decisions = {'ARRAY_TYPE': 'native'}

        # Build enum lookup from schema
        enum_types = set()
        for enum in schema.get('enums', []):
            enum_types.add(enum['enum_name'].lower())

        transformed_schema = schema.copy()

        for table in transformed_schema.get('tables', []):
            for column in table.get('columns', []):
                original_type = column['data_type']

                # Transform the type
                transformed_type, warnings = self._transform_single_type(
                    original_type,
                    decisions,
                    table['table_name'],
                    column['column_name'],
                    enum_types
                )

                # Update column
                column['data_type'] = transformed_type
                if original_type != transformed_type:
                    column['original_type'] = original_type
                    self._log_transformation(
                        table['table_name'],
                        column['column_name'],
                        original_type,
                        transformed_type
                    )

                # Collect warnings
                self.warnings_generated.extend(warnings)

        # Update schema metadata
        transformed_schema['type_transformations'] = self.transformations_applied
        transformed_schema['type_warnings'] = self.warnings_generated

        return transformed_schema

    def _transform_single_type(self, pg_type: str, decisions: Dict,
                             table_name: str, column_name: str, enum_types: set = None) -> Tuple[str, List[str]]:
        """Transform a single PostgreSQL type to DBML."""

        warnings = []
        enum_types = enum_types or set()

        # Handle enum types first (preserve custom types)
        base_type, params = self._extract_type_components(pg_type)
        if base_type.lower() in enum_types:
            return base_type, warnings  # Keep enum type as-is

        # Handle array types
        if self._is_array_type(pg_type):
            return self._handle_array_type(pg_type, decisions, table_name, column_name)

        # Handle types with parameters (e.g., varchar(255), numeric(10,2))
        base_type, params = self._extract_type_components(pg_type)

        # Direct mapping
        if base_type.lower() in self.type_map:
            dbml_type = self.type_map[base_type.lower()]

            # Preserve parameters where appropriate
            if params and dbml_type in ['varchar', 'char', 'numeric', 'decimal']:
                dbml_type = f"{dbml_type}({params})"

            # Generate warnings for semantic loss types
            semantic_loss_types = {
                'int4multirange': 'PostgreSQL multirange types not supported in DBML',
                'int8multirange': 'PostgreSQL multirange types not supported in DBML',
                'nummultirange': 'PostgreSQL multirange types not supported in DBML',
                'tsmultirange': 'PostgreSQL multirange types not supported in DBML',
                'datemultirange': 'PostgreSQL multirange types not supported in DBML',
                'hstore': 'PostgreSQL hstore extension type causes semantic loss in DBML',
                'ltree': 'PostgreSQL ltree extension type causes semantic loss in DBML',
                'cube': 'PostgreSQL cube extension type causes semantic loss in DBML',
                'isbn': 'PostgreSQL ISBN extension type causes semantic loss in DBML',
                'issn': 'PostgreSQL ISSN extension type causes semantic loss in DBML'
            }

            if base_type.lower() in semantic_loss_types:
                warnings.append(
                    f"Type '{pg_type}' converted to 'text' - {semantic_loss_types[base_type.lower()]}"
                )

            return dbml_type, warnings

        # Unknown type - fallback to text
        warnings.append(f"Unknown type '{pg_type}' converted to 'text'")
        return 'text', warnings

    def _handle_array_type(self, pg_type: str, decisions: Dict,
                          table_name: str, column_name: str) -> Tuple[str, List[str]]:
        """
        Handle array types with native DBML array support.

        Updated: dbdiagram.io supports array types with quoted syntax: "type[]"
        """

        warnings = []
        strategy = decisions.get('ARRAY_TYPE', 'native')

        if strategy == 'native':
            result_type = self._convert_native_array_type(pg_type)
            # No warning needed - native support
        elif strategy == 'text_fallback':
            result_type = 'text'
            warnings.append(f"Array type '{pg_type}' flattened to text - array semantics lost")
        else:
            result_type = self._convert_native_array_type(pg_type)

        return result_type, warnings

    def _convert_native_array_type(self, pg_type: str) -> str:
        """Convert array type to native DBML array syntax."""
        # Extract element type
        element_type = pg_type.replace('[]', '').strip()

        # Map element type to DBML equivalent
        mapped_element = self.type_map.get(element_type.lower(), element_type)

        # Return quoted array notation: "type[]"
        return f'"{mapped_element}[]"'

    def _is_array_type(self, pg_type: str) -> bool:
        """Check if type is an array type."""
        return '[]' in pg_type

    def _extract_type_components(self, pg_type: str) -> Tuple[str, str]:
        """Extract base type and parameters from type string."""

        # Handle array notation
        is_array = '[]' in pg_type
        if is_array:
            pg_type = pg_type.replace('[]', '').strip()

        # Extract parameters
        match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]+)\)', pg_type)
        if match:
            base_type = match.group(1)
            params = match.group(2)
        else:
            base_type = pg_type.strip()
            params = None

        if is_array:
            base_type += '[]'

        return base_type, params

    def _log_transformation(self, table_name: str, column_name: str,
                          original_type: str, transformed_type: str):
        """Log type transformation for reporting."""

        self.transformations_applied.append({
            'table': table_name,
            'column': column_name,
            'original_type': original_type,
            'transformed_type': transformed_type,
            'transformation_reason': self._get_transformation_reason(original_type, transformed_type)
        })

    def _get_transformation_reason(self, original: str, transformed: str) -> str:
        """Get human-readable reason for transformation."""

        if '[]' in original:
            return 'Array type syntax incompatibility'
        elif original.lower() in ['inet', 'cidr', 'macaddr', 'tsvector', 'xml']:
            return 'PostgreSQL-specific type not supported in DBML'
        elif 'with time zone' in original or 'without time zone' in original:
            return 'Multi-word type converted to alias'
        else:
            return 'DBML compatibility'

    def get_transformation_report(self) -> Dict:
        """Generate comprehensive transformation report."""

        # Group transformations by type
        by_reason = {}
        for transform in self.transformations_applied:
            reason = transform['transformation_reason']
            if reason not in by_reason:
                by_reason[reason] = []
            by_reason[reason].append(transform)

        return {
            'total_transformations': len(self.transformations_applied),
            'total_warnings': len(self.warnings_generated),
            'by_reason': by_reason,
            'warnings': self.warnings_generated,
            'all_transformations': self.transformations_applied
        }
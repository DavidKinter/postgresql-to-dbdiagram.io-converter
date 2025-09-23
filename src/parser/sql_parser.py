"""
PostgreSQL SQL parsing for schema extraction.
"""

import sqlparse
import re
from typing import Dict, List, Optional, Any
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Keyword, Name

class SQLParser:
    """Parse PostgreSQL SQL dumps to extract schema information."""

    def __init__(self):
        self.tables = []
        self.relationships = []
        self.constraints = []
        self.indexes = []
        self.sequences = []
        self.enums = []
        self.parsing_errors = []

    def parse_sql_dump(self, sql_content: str) -> Dict[str, Any]:
        """
        Parse SQL dump and extract schema components.

        Returns structured schema data for transformation.
        """

        try:
            # First try to extract individual CREATE TABLE statements directly
            self._extract_create_table_statements(sql_content)

            # Then parse remaining statements normally (but skip CREATE TABLE to avoid duplicates)
            statements = sqlparse.parse(sql_content)
            for statement in statements:
                self._parse_statement(statement)

        except Exception as e:
            self.parsing_errors.append({
                'error': str(e),
                'context': 'SQL parsing failed'
            })

        return {
            'tables': self.tables,
            'relationships': self.relationships,
            'constraints': self.constraints,
            'indexes': self.indexes,
            'sequences': self.sequences,
            'enums': self.enums,
            'parsing_errors': self.parsing_errors
        }

    def _extract_create_table_statements(self, sql_content: str):
        """Extract CREATE TABLE statements directly from SQL content."""
        # Find all CREATE TABLE statements using regex
        pattern = r'CREATE\s+TABLE\s+[^;]+;'
        matches = re.finditer(pattern, sql_content, re.IGNORECASE | re.DOTALL)

        for match in matches:
            table_statement = match.group(0)
            try:
                # Parse this individual CREATE TABLE statement
                statements = sqlparse.parse(table_statement)
                if statements:
                    self._parse_create_table(statements[0])
            except Exception as e:
                self.parsing_errors.append({
                    'error': str(e),
                    'statement': table_statement[:200],
                    'context': 'CREATE TABLE parsing failed'
                })

    def _parse_statement(self, statement: Statement):
        """Parse individual SQL statement."""
        statement_str = str(statement).strip()

        if not statement_str:
            return

        statement_upper = statement_str.upper()

        try:
            if statement_upper.startswith('CREATE TABLE'):
                # Skip CREATE TABLE - already handled by _extract_create_table_statements
                pass
            elif statement_upper.startswith('ALTER TABLE') or 'ALTER TABLE' in statement_upper:
                # Handle both direct ALTER TABLE and comment-prefixed ALTER TABLE statements
                self._parse_alter_table(statement)
            elif statement_upper.startswith('CREATE INDEX'):
                self._parse_create_index(statement)
            elif statement_upper.startswith('CREATE SEQUENCE'):
                self._parse_create_sequence(statement)
            elif ('CREATE TYPE' in statement_upper and 'ENUM' in statement_upper):
                self._parse_create_enum(statement)
        except Exception as e:
            self.parsing_errors.append({
                'error': str(e),
                'statement': statement_str[:200],
                'context': 'Statement parsing failed'
            })

    def _parse_create_table(self, statement: Statement):
        """Parse CREATE TABLE statement."""
        statement_str = str(statement)

        # Extract table name
        table_name = self._extract_table_name(statement_str)
        if not table_name:
            return

        # Check if this is a PARTITION OF or INHERITS table
        is_partition = 'PARTITION OF' in statement_str.upper()
        is_inheritance = 'INHERITS' in statement_str.upper()

        # Extract columns (handle special cases for partition/inheritance tables)
        if is_partition:
            columns = []  # Partition tables inherit columns from parent
        elif is_inheritance:
            # For INHERITS tables, still extract defined columns but they also inherit from parent
            columns = self._extract_columns(statement_str)
        else:
            columns = self._extract_columns(statement_str)

        # Extract table-level constraints
        table_constraints = self._extract_table_constraints(statement_str, table_name)

        table_info = {
            'name': table_name,
            'table_name': table_name,  # Keep for backward compatibility
            'columns': columns,
            'constraints': table_constraints,
            'original_statement': statement_str
        }

        # Check for duplicate tables before adding
        existing_table_names = [t['table_name'] for t in self.tables]
        if table_name not in existing_table_names:
            self.tables.append(table_info)

    def _extract_table_name(self, statement: str) -> Optional[str]:
        """Extract table name from CREATE TABLE statement."""
        # Updated regex to handle quoted identifiers with any characters inside
        match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("(?:[^"]|"")*"|[\w.]+)',
                         statement, re.IGNORECASE | re.UNICODE)
        if match:
            table_name = match.group(1)
            # Only strip quotes if the entire name is quoted
            if table_name.startswith('"') and table_name.endswith('"'):
                table_name = table_name[1:-1]
                # Handle escaped quotes
                table_name = table_name.replace('""', '"')
            # Remove schema prefix if present
            if '.' in table_name and not table_name.startswith('"'):
                table_name = table_name.split('.')[-1]
            return table_name
        return None

    def _extract_columns(self, statement: str) -> List[Dict]:
        """Extract column definitions from CREATE TABLE statement."""
        columns = []

        # Find the column definitions between parentheses
        # Handle tables with WITH clauses, storage parameters, etc.
        match = re.search(r'CREATE\s+TABLE\s+[^(]+\((.*?)\)\s*(?:WITH|TABLESPACE|;|$)',
                         statement, re.IGNORECASE | re.DOTALL)
        if not match:
            return columns

        columns_section = match.group(1)

        # Split by commas, but be careful with nested parentheses
        column_parts = self._split_column_definitions(columns_section)

        for part in column_parts:
            part = part.strip()
            if not part or part.upper().startswith(('CONSTRAINT', 'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'EXCLUDE')):
                continue

            column_info = self._parse_column_definition(part)
            if column_info:
                columns.append(column_info)

        return columns

    def _split_column_definitions(self, columns_section: str) -> List[str]:
        """Split column definitions by commas, respecting parentheses."""
        # Use regex to find complete CONSTRAINT definitions first
        constraint_pattern = r'CONSTRAINT\s+\w+\s+(?:(?:FOREIGN\s+KEY|PRIMARY\s+KEY|UNIQUE|CHECK).*?)(?=,\s*(?:CONSTRAINT|\w+\s+\w+|$)|\s*$)'

        # Find all constraint matches
        constraint_matches = list(re.finditer(constraint_pattern, columns_section, re.IGNORECASE | re.DOTALL))

        if not constraint_matches:
            # No constraints found, use simple splitting
            return self._split_column_definitions_simple(columns_section)

        parts = []
        last_end = 0

        for match in constraint_matches:
            # Add everything before this constraint
            before_constraint = columns_section[last_end:match.start()].strip()
            if before_constraint:
                # Split the before part using simple logic
                before_parts = self._split_column_definitions_simple(before_constraint.rstrip(','))
                parts.extend(p.strip() for p in before_parts if p.strip())

            # Add the complete constraint
            constraint_text = match.group(0).strip()
            if constraint_text:
                parts.append(constraint_text)

            last_end = match.end()

        # Add anything remaining after the last constraint
        remaining = columns_section[last_end:].strip()
        if remaining:
            remaining_parts = self._split_column_definitions_simple(remaining.lstrip(','))
            parts.extend(p.strip() for p in remaining_parts if p.strip())

        return parts

    def _split_column_definitions_simple(self, columns_section: str) -> List[str]:
        """Original simple split logic as fallback."""
        parts = []
        current_part = ""
        paren_depth = 0

        for char in columns_section:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                parts.append(current_part)
                current_part = ""
                continue

            current_part += char

        if current_part.strip():
            parts.append(current_part)

        return parts

    def _parse_column_definition(self, column_def: str) -> Optional[Dict]:
        """Parse individual column definition."""
        # Handle quoted identifiers properly
        column_def = column_def.strip()

        # Handle multi-line parts with comments and column definitions
        if '\n' in column_def:
            # Split into lines and find the actual column definition
            lines = column_def.split('\n')
            actual_column_line = None

            for line in lines:
                line_stripped = line.strip()
                if (line_stripped and
                    not line_stripped.startswith('--') and
                    not line_stripped.startswith('/*') and
                    not line_stripped.upper().startswith(('CONSTRAINT', 'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'EXCLUDE')) and
                    ' WITH ' not in line_stripped.upper()):  # Exclude constraint parts
                    # This looks like a column definition
                    actual_column_line = line_stripped
                    break

            if actual_column_line:
                column_def = actual_column_line
            else:
                return None

        # Skip comment lines or empty lines
        if not column_def or column_def.startswith('--') or column_def.startswith('/*'):
            return None

        # Skip partition-specific clauses that might be misinterpreted as columns
        if any(keyword in column_def.upper() for keyword in ['FOR VALUES', 'VALUES FROM', 'VALUES IN', 'TO (']):
            return None

        # Extract column name (may be quoted)
        if column_def.startswith('"'):
            # Find the closing quote
            end_quote = column_def.find('"', 1)
            if end_quote == -1:
                return None
            column_name = column_def[1:end_quote]
            remaining = column_def[end_quote+1:].strip()
        else:
            # Unquoted identifier
            parts = column_def.split(None, 1)
            if len(parts) < 2:
                return None
            column_name = parts[0]
            remaining = parts[1]

        # Extract data type
        parts = remaining.split()
        if not parts:
            return None
        data_type = parts[0]

        # Handle type parameters like VARCHAR(255)
        if '(' in data_type and ')' in data_type:
            type_match = re.match(r'(\w+)\(([^)]+)\)', data_type)
            if type_match:
                base_type = type_match.group(1)
                type_params = type_match.group(2)
                data_type = f"{base_type}({type_params})"

        # Parse column constraints
        constraints = []
        default_value = None
        is_nullable = True

        column_def_upper = column_def.upper()

        if 'NOT NULL' in column_def_upper:
            is_nullable = False
            constraints.append('NOT NULL')

        if 'PRIMARY KEY' in column_def_upper:
            constraints.append('PRIMARY KEY')

        if 'UNIQUE' in column_def_upper:
            constraints.append('UNIQUE')

        # Extract default value
        default_match = re.search(r'DEFAULT\s+([^,\s]+(?:\([^)]*\))?)',
                                column_def, re.IGNORECASE)
        if default_match:
            default_value = default_match.group(1)

        return {
            'column_name': column_name,
            'data_type': data_type,
            'is_nullable': is_nullable,
            'default_value': default_value,
            'constraints': constraints,
            'original_definition': column_def.strip()
        }

    def _extract_table_constraints(self, statement: str, table_name: str) -> List[Dict]:
        """Extract table-level constraints."""
        constraints = []

        # Find constraint definitions in CREATE TABLE
        # Handle tables with WITH clauses, storage parameters, etc.
        match = re.search(r'CREATE\s+TABLE\s+[^(]+\((.*?)\)\s*(?:WITH|TABLESPACE|;|$)',
                         statement, re.IGNORECASE | re.DOTALL)
        if not match:
            return constraints

        columns_section = match.group(1)
        parts = self._split_column_definitions(columns_section)

        for part in parts:
            part = part.strip()
            part_upper = part.upper()

            if part_upper.startswith('CONSTRAINT'):
                constraint_info = self._parse_table_constraint(part, table_name)
                if constraint_info:
                    constraints.append(constraint_info)
            elif part_upper.startswith(('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')):
                constraint_info = self._parse_inline_constraint(part, table_name)
                if constraint_info:
                    constraints.append(constraint_info)

        return constraints

    def _parse_table_constraint(self, constraint_def: str, table_name: str) -> Optional[Dict]:
        """Parse named table constraint."""
        constraint_def = constraint_def.strip()

        # Extract constraint name
        name_match = re.match(r'CONSTRAINT\s+(["\w]+)\s+(.*)',
                            constraint_def, re.IGNORECASE | re.DOTALL)
        if not name_match:
            return None

        constraint_name = name_match.group(1).strip('"')
        constraint_body = name_match.group(2).strip()

        return self._parse_constraint_body(constraint_body, table_name, constraint_name)

    def _parse_inline_constraint(self, constraint_def: str, table_name: str) -> Optional[Dict]:
        """Parse inline constraint without explicit name."""
        return self._parse_constraint_body(constraint_def, table_name, None)

    def _parse_constraint_body(self, constraint_body: str, table_name: str,
                             constraint_name: Optional[str]) -> Optional[Dict]:
        """Parse constraint body to extract type and details."""
        constraint_body = constraint_body.strip()
        constraint_upper = constraint_body.upper()

        if constraint_upper.startswith('PRIMARY KEY'):
            columns_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)',
                                    constraint_body, re.IGNORECASE)
            columns = []
            if columns_match:
                columns = [col.strip().strip('"') for col in columns_match.group(1).split(',')]

            return {
                'constraint_name': constraint_name,
                'constraint_type': 'p',
                'table_name': table_name,
                'columns': columns,
                'definition': constraint_body
            }

        elif constraint_upper.startswith('FOREIGN KEY'):
            return self._parse_foreign_key_constraint(constraint_body, table_name, constraint_name)

        elif constraint_upper.startswith('UNIQUE'):
            columns_match = re.search(r'UNIQUE\s*\(([^)]+)\)',
                                    constraint_body, re.IGNORECASE)
            columns = []
            if columns_match:
                columns = [col.strip().strip('"') for col in columns_match.group(1).split(',')]

            return {
                'constraint_name': constraint_name,
                'constraint_type': 'u',
                'table_name': table_name,
                'columns': columns,
                'definition': constraint_body
            }

        elif constraint_upper.startswith('CHECK'):
            return {
                'constraint_name': constraint_name,
                'constraint_type': 'c',
                'table_name': table_name,
                'check_expression': constraint_body,
                'definition': constraint_body
            }

        return None

    def _parse_foreign_key_constraint(self, constraint_body: str, table_name: str,
                                    constraint_name: Optional[str]) -> Optional[Dict]:
        """Parse foreign key constraint."""
        # FOREIGN KEY (columns) REFERENCES target_table (target_columns)
        fk_match = re.search(
            r'FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(["\w.]+)\s*\(([^)]+)\)',
            constraint_body, re.IGNORECASE
        )

        if not fk_match:
            return None

        source_columns = [col.strip().strip('"') for col in fk_match.group(1).split(',')]
        target_table = fk_match.group(2).strip('"')
        target_columns = [col.strip().strip('"') for col in fk_match.group(3).split(',')]

        # Remove schema prefix if present
        if '.' in target_table:
            target_table = target_table.split('.')[-1]

        # Extract ON DELETE/UPDATE actions
        on_delete_action = None
        on_update_action = None

        delete_match = re.search(r'ON\s+DELETE\s+(CASCADE|RESTRICT|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION)',
                               constraint_body, re.IGNORECASE)
        if delete_match:
            on_delete_action = delete_match.group(1).upper()

        update_match = re.search(r'ON\s+UPDATE\s+(CASCADE|RESTRICT|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION)',
                               constraint_body, re.IGNORECASE)
        if update_match:
            on_update_action = update_match.group(1).upper()

        constraint_info = {
            'constraint_name': constraint_name,
            'constraint_type': 'f',
            'table_name': table_name,
            'columns': source_columns,
            'referenced_table': target_table,
            'referenced_columns': target_columns,
            'on_delete_action': on_delete_action,
            'on_update_action': on_update_action,
            'definition': constraint_body
        }

        # Also add to relationships
        relationship = {
            'source_table': table_name,
            'source_columns': source_columns,
            'target_table': target_table,
            'target_columns': target_columns,
            'relationship_type': 'many-to-one',
            'on_delete_action': on_delete_action,
            'on_update_action': on_update_action,
            'constraint_name': constraint_name
        }

        self.relationships.append(relationship)

        return constraint_info

    def _parse_alter_table(self, statement: Statement):
        """Parse ALTER TABLE statement."""
        statement_str = str(statement)

        # Extract table name (handle ONLY keyword)
        table_match = re.search(r'ALTER\s+TABLE\s+(?:ONLY\s+)?(["\w.]+)',
                              statement_str, re.IGNORECASE)
        if not table_match:
            return

        table_name = table_match.group(1).strip('"')
        if '.' in table_name:
            table_name = table_name.split('.')[-1]

        # Handle ADD COLUMN
        if 'ADD COLUMN' in statement_str.upper():
            self._parse_add_column(statement_str, table_name)

        # Handle ADD CONSTRAINT
        if 'ADD CONSTRAINT' in statement_str.upper():
            self._parse_add_constraint(statement_str, table_name)

    def _parse_add_column(self, statement: str, table_name: str):
        """Parse ALTER TABLE ADD COLUMN statement."""
        # Extract ADD COLUMN definitions
        add_column_match = re.search(r'ADD\s+COLUMN\s+([^,;]+)', statement, re.IGNORECASE)
        if not add_column_match:
            return

        column_def = add_column_match.group(1).strip()

        # Parse the column definition
        parsed_column = self._parse_column_definition(column_def)
        if parsed_column:
            # Find the table and add the column to it
            for table in self.tables:
                if table['table_name'] == table_name:
                    # Check if column already exists
                    existing_columns = [col['column_name'] for col in table.get('columns', [])]
                    if parsed_column['column_name'] not in existing_columns:
                        table.setdefault('columns', []).append(parsed_column)
                    break

    def _parse_add_constraint(self, statement: str, table_name: str):
        """Parse ALTER TABLE ADD CONSTRAINT statement."""
        constraint_match = re.search(
            r'ADD\s+CONSTRAINT\s+(["\w]+)\s+(.*?)(?:;|\s*$)',
            statement, re.IGNORECASE | re.DOTALL
        )

        if constraint_match:
            constraint_name = constraint_match.group(1).strip('"')
            constraint_body = constraint_match.group(2)

            constraint_info = self._parse_constraint_body(constraint_body, table_name, constraint_name)
            if constraint_info:
                self.constraints.append(constraint_info)

    def _parse_create_index(self, statement: Statement):
        """Parse CREATE INDEX statement with enhanced metadata extraction."""
        statement_str = str(statement)

        # Enhanced regex to capture USING clause
        index_match = re.search(
            r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(["\w]+)\s+ON\s+(["\w.]+)\s*(?:USING\s+(\w+)\s*)?\(([^)]+)\)',
            statement_str, re.IGNORECASE
        )

        if index_match:
            index_name = index_match.group(1).strip('"')
            table_name = index_match.group(2).strip('"')
            index_method = index_match.group(3)  # btree, gin, gist, etc.
            columns_str = index_match.group(4)

            if '.' in table_name:
                table_name = table_name.split('.')[-1]

            columns = [col.strip().strip('"') for col in columns_str.split(',')]

            is_unique = 'UNIQUE' in statement_str.upper()

            # Default to btree if no USING clause specified
            if not index_method:
                index_method = 'btree'

            index_info = {
                'index_name': index_name,
                'table_name': table_name,
                'columns': columns,
                'is_unique': is_unique,
                'index_method': index_method.lower() if index_method else 'btree',
                'definition': statement_str
            }

            self.indexes.append(index_info)

    def _parse_create_enum(self, statement: Statement):
        """Parse CREATE TYPE ... AS ENUM statement."""
        statement_str = str(statement)

        # Match CREATE TYPE name AS ENUM ('value1', 'value2', ...)
        enum_match = re.search(
            r'CREATE\s+TYPE\s+(["\w]+)\s+AS\s+ENUM\s*\(([^)]+)\)',
            statement_str, re.IGNORECASE | re.DOTALL
        )

        if enum_match:
            enum_name = enum_match.group(1).strip().strip('"')
            values_str = enum_match.group(2)

            # Extract individual enum values
            values = []
            # Split by comma, but handle quoted values properly
            for value in re.findall(r"'([^']*)'", values_str):
                values.append(value)

            if values:
                enum_info = {
                    'enum_name': enum_name,
                    'values': values,
                    'definition': statement_str
                }
                self.enums.append(enum_info)

    def _parse_create_sequence(self, statement: Statement):
        """Parse CREATE SEQUENCE statement."""
        statement_str = str(statement)

        sequence_match = re.search(r'CREATE\s+SEQUENCE\s+(["\w.]+)',
                                 statement_str, re.IGNORECASE)
        if sequence_match:
            sequence_name = sequence_match.group(1).strip('"')
            if '.' in sequence_name:
                sequence_name = sequence_name.split('.')[-1]

            sequence_info = {
                'sequence_name': sequence_name,
                'definition': statement_str
            }

            self.sequences.append(sequence_info)
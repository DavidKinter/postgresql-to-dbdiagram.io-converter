"""
Parser preparation utilities to ensure SQL is ready for DBML parsing.
"""

import re
from typing import Dict, List

class ParserPrep:
    """Prepare cleaned SQL for optimal DBML parser compatibility."""

    def __init__(self):
        self.modifications = []

    def prepare_for_parser(self, cleaned_sql: str) -> str:
        """
        Apply final preparations to cleaned SQL for DBML parser.
        """

        prepared_sql = cleaned_sql

        # Normalize whitespace around statements
        prepared_sql = self._normalize_whitespace(prepared_sql)

        # Fix table settings syntax for 2024 parser
        prepared_sql = self._fix_table_settings_syntax(prepared_sql)

        # Ensure proper statement termination
        prepared_sql = self._fix_statement_termination(prepared_sql)

        # Remove empty lines that can confuse parser
        prepared_sql = self._remove_excessive_whitespace(prepared_sql)

        return prepared_sql

    def _normalize_whitespace(self, sql: str) -> str:
        """Normalize whitespace for parser consistency."""
        # Ensure single space around keywords
        sql = re.sub(r'\s+', ' ', sql)

        # Ensure proper line breaks before major statements
        keywords = ['CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE']
        for keyword in keywords:
            pattern = rf'(\S)\s*({keyword}\s+)'
            replacement = r'\1\n\2'
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

        return sql

    def _fix_table_settings_syntax(self, sql: str) -> str:
        """Fix table settings syntax for 2024 parser requirements."""
        # January 2024 parser requires whitespace before table settings
        # Table users[headercolor: #3498DB] -> Table users [headercolor: #3498DB]
        pattern = r'(Table\s+\w+)\[([^\]]+)\]'
        replacement = r'\1 [\2]'
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

        self.modifications.append({
            'type': 'table_settings_spacing',
            'description': 'Added required whitespace before table settings brackets'
        })

        return sql

    def _fix_statement_termination(self, sql: str) -> str:
        """Ensure proper statement termination."""
        # Add semicolons where missing for major statements
        lines = sql.split('\n')
        fixed_lines = []

        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.endswith((';', ',', ')', '{')):
                # Check if this looks like a complete statement
                if any(stripped.upper().startswith(kw) for kw in
                      ['CREATE TABLE', 'CREATE INDEX', 'ALTER TABLE']):
                    if ')' in line and not line.strip().endswith(';'):
                        line = line.rstrip() + ';'

            fixed_lines.append(line)

        return '\n'.join(fixed_lines)

    def _remove_excessive_whitespace(self, sql: str) -> str:
        """Remove excessive whitespace that can confuse parser."""
        # Remove multiple consecutive empty lines
        sql = re.sub(r'\n\s*\n\s*\n', '\n\n', sql)

        # Remove trailing whitespace from lines
        lines = [line.rstrip() for line in sql.split('\n')]

        return '\n'.join(lines)

    def get_preparation_report(self) -> Dict:
        """Get report of all modifications made during preparation."""
        return {
            'total_modifications': len(self.modifications),
            'modifications': self.modifications
        }
"""
SQL dump preprocessing to remove PostgreSQL-specific statements
that cause dbdiagram.io parser failures.

Based on incompatibility analysis lines 70-77.
"""

import re
from typing import Dict, List, Tuple

class SQLCleaner:
    """Remove PostgreSQL-specific statements that break DBML parsing."""

    def __init__(self):
        self.removed_statements = []
        self.line_mapping = {}

    def clean_dump(self, sql_content: str) -> str:
        """
        Clean PostgreSQL dump for DBML compatibility.

        Addresses specific issues from incompatibility analysis:
        - Line 70-72: SET statements block import
        - Line 74: COMMENT ON statements ignored/error
        - Line 77: COPY statements unsupported
        - Line 66: Dollar-quoted strings break parser
        """

        lines = sql_content.split('\n')
        cleaned_lines = []

        for i, line in enumerate(lines):
            original_line = i + 1

            if not self._should_remove_line(line):
                self.line_mapping[len(cleaned_lines) + 1] = original_line
                cleaned_lines.append(self._clean_line(line, original_line))
            else:
                self._log_removal(line, original_line)

        cleaned_content = '\n'.join(cleaned_lines)

        # Multi-line statement cleaning
        cleaned_content = self._remove_multiline_statements(cleaned_content)

        return cleaned_content

    def _should_remove_line(self, line: str) -> bool:
        """Check if line should be completely removed."""
        line_stripped = line.strip()

        if not line_stripped or line_stripped.startswith('--'):
            return False

        # PostgreSQL dump security commands
        if line_stripped.startswith('\\'):
            return True

        # SET statements (Lines 70-72)
        if line_stripped.upper().startswith('SET '):
            return True

        # SELECT statements (including pg_catalog)
        if line_stripped.upper().startswith('SELECT '):
            return True

        # COMMENT ON statements (Line 74)
        if line_stripped.upper().startswith('COMMENT ON '):
            return True

        # COPY statements (Line 77) and backslash copy end marker
        if line_stripped.upper().startswith('COPY ') or line_stripped == '\\.':
            return True

        # GRANT/REVOKE statements (Line 74)
        if line_stripped.upper().startswith(('GRANT ', 'REVOKE ')):
            return True

        # ALTER ... OWNER TO statements (Line 74)
        if 'OWNER TO' in line_stripped.upper():
            return True

        # ALTER SEQUENCE ... OWNED BY statements
        if 'OWNED BY' in line_stripped.upper():
            return True

        # ALTER TABLE ONLY statements for defaults
        if line_stripped.upper().startswith('ALTER TABLE ONLY ') and 'SET DEFAULT' in line_stripped.upper():
            return True

        return False

    def _clean_line(self, line: str, line_number: int) -> str:
        """Clean individual line while preserving it."""
        cleaned = line

        # Fix negative defaults (Line 24)
        cleaned = re.sub(r'DEFAULT\s+(-\d+)', r"DEFAULT '\1'", cleaned, flags=re.IGNORECASE)

        # Fix UUID function calls (Line 16)
        if 'gen_random_uuid()' in cleaned:
            cleaned = cleaned.replace('gen_random_uuid()', '`uuid_generate_v4()`')

        # Convert multi-word types (Line 13)
        type_replacements = {
            'timestamp with time zone': 'timestamptz',
            'timestamp without time zone': 'timestamp',
            'time with time zone': 'timetz',
            'time without time zone': 'time',
            'double precision': 'float8',
            'character varying': 'varchar'
        }

        for old_type, new_type in type_replacements.items():
            cleaned = re.sub(
                rf'\b{re.escape(old_type)}\b',
                new_type,
                cleaned,
                flags=re.IGNORECASE
            )

        # Quote array types (Line 10)
        cleaned = re.sub(r'\b(\w+)\[\]', r'"\1 []"', cleaned)

        return cleaned

    def _remove_multiline_statements(self, content: str) -> str:
        """Remove multi-line statements that break parsing."""

        # Dollar-quoted strings (Line 66)
        content = re.sub(
            r'\$\$.*?\$\$',
            '',
            content,
            flags=re.DOTALL
        )

        # Remove CREATE FUNCTION/PROCEDURE statements
        content = re.sub(
            r'CREATE\s+(OR\s+REPLACE\s+)?(FUNCTION|PROCEDURE)\s+.*?;',
            '',
            content,
            flags=re.DOTALL | re.IGNORECASE
        )

        # Remove complex constraint definitions (Line 38)
        content = re.sub(
            r'CHECK\s*\([^)]*\([^)]*\)[^)]*\)',
            '',
            content,
            flags=re.IGNORECASE
        )

        return content

    def _log_removal(self, line: str, line_number: int):
        """Log removed statement for reporting."""
        statement_type = self._identify_statement_type(line)

        self.removed_statements.append({
            'line_number': line_number,
            'statement_type': statement_type,
            'content': line.strip()[:100],
            'reason': self._get_removal_reason(statement_type)
        })

    def _identify_statement_type(self, line: str) -> str:
        """Identify the type of SQL statement."""
        line_stripped = line.strip()
        line_upper = line_stripped.upper()

        if line_stripped.startswith('\\'):
            return 'PSQL_COMMAND'
        elif line_upper.startswith('SET '):
            return 'SET'
        elif line_upper.startswith('SELECT '):
            return 'SELECT'
        elif line_upper.startswith('COMMENT ON '):
            return 'COMMENT'
        elif line_upper.startswith('COPY ') or line_stripped == '\\.':
            return 'COPY'
        elif line_upper.startswith(('GRANT ', 'REVOKE ')):
            return 'PERMISSION'
        elif 'OWNER TO' in line_upper:
            return 'OWNERSHIP'
        elif 'OWNED BY' in line_upper:
            return 'SEQUENCE_OWNERSHIP'
        elif line_upper.startswith('ALTER TABLE ONLY ') and 'SET DEFAULT' in line_upper:
            return 'COLUMN_DEFAULT'
        else:
            return 'UNKNOWN'

    def _get_removal_reason(self, statement_type: str) -> str:
        """Get human-readable reason for removal."""
        reasons = {
            'PSQL_COMMAND': 'PostgreSQL client commands not supported in DBML',
            'SET': 'Configuration statements not supported in DBML',
            'SELECT': 'Query statements not supported in DBML',
            'COMMENT': 'Comment statements cause parser errors',
            'COPY': 'Data import statements not supported',
            'PERMISSION': 'Permission statements not represented in DBML',
            'OWNERSHIP': 'Ownership statements not represented in DBML',
            'SEQUENCE_OWNERSHIP': 'Sequence ownership not represented in DBML',
            'COLUMN_DEFAULT': 'Column defaults handled in CREATE TABLE',
            'UNKNOWN': 'Statement incompatible with DBML parser'
        }
        return reasons.get(statement_type, 'Unknown incompatibility')

    def get_removal_report(self) -> Dict:
        """Generate report of all removed statements."""
        by_type = {}
        for stmt in self.removed_statements:
            stmt_type = stmt['statement_type']
            if stmt_type not in by_type:
                by_type[stmt_type] = []
            by_type[stmt_type].append(stmt)

        return {
            'total_removed': len(self.removed_statements),
            'by_type': by_type,
            'line_mapping': self.line_mapping
        }
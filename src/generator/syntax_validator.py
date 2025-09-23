"""
DBML syntax validation for 2024 parser compliance.
"""

import re
from typing import Dict, List, Any, Tuple

class SyntaxValidator:
    """Validate DBML syntax for dbdiagram.io 2024 parser compatibility."""

    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []

    def validate_dbml(self, dbml_content: str) -> Dict[str, Any]:
        """
        Validate DBML content for 2024 parser compliance.

        Returns validation report with errors and warnings.
        """

        self.validation_errors = []
        self.validation_warnings = []

        lines = dbml_content.split('\n')

        for line_num, line in enumerate(lines, 1):
            self._validate_line(line, line_num)

        # Validate overall structure
        self._validate_overall_structure(dbml_content)

        return {
            'is_valid': len(self.validation_errors) == 0,
            'total_errors': len(self.validation_errors),
            'total_warnings': len(self.validation_warnings),
            'errors': self.validation_errors,
            'warnings': self.validation_warnings
        }

    def _validate_line(self, line: str, line_num: int):
        """Validate individual line for common syntax issues."""

        line_stripped = line.strip()

        if not line_stripped or line_stripped.startswith('//'):
            return

        # Check for table settings spacing (2024 requirement)
        if re.match(r'Table\s+\w+\[', line_stripped):
            self._add_error(
                line_num,
                "TABLE_SETTINGS_SPACING",
                "Table settings require space before bracket. Use 'Table name [settings]' not 'Table name[settings]'",
                line_stripped
            )

        # Check for proper bracket placement
        if '\n[' in line or line_stripped.startswith('[') and not line_stripped.startswith('[headercolor'):
            self._add_warning(
                line_num,
                "BRACKET_PLACEMENT",
                "Line breaks before brackets can cause parser issues",
                line_stripped
            )

        # Check for unquoted negative defaults
        if re.search(r'default:\s*-\d+(?!\w)', line_stripped):
            self._add_error(
                line_num,
                "UNQUOTED_NEGATIVE_DEFAULT",
                "Negative default values must be quoted: default: '-1' not default: -1",
                line_stripped
            )

        # Check for array syntax issues
        if re.search(r'\w+\[\](?!\s)', line_stripped) and '"' not in line_stripped:
            self._add_error(
                line_num,
                "UNQUOTED_ARRAY_TYPE",
                "Array types must be quoted: \"type []\" not type[]",
                line_stripped
            )

        # Check for multi-word types
        multi_word_types = [
            r'double\s+precision',
            r'timestamp\s+with\s+time\s+zone',
            r'timestamp\s+without\s+time\s+zone',
            r'character\s+varying'
        ]

        for pattern in multi_word_types:
            if re.search(pattern, line_stripped, re.IGNORECASE):
                self._add_error(
                    line_num,
                    "MULTI_WORD_TYPE",
                    f"Multi-word types not supported. Use aliases: 'double precision' -> 'float8'",
                    line_stripped
                )

        # Check for function calls without backticks
        if re.search(r'default:\s*\w+\(\)', line_stripped) and '`' not in line_stripped:
            self._add_error(
                line_num,
                "UNQUOTED_FUNCTION_CALL",
                "Function calls in defaults must use backticks: `function()` not function()",
                line_stripped
            )

        # Check for incomplete relationship definitions
        if line_stripped.startswith('Ref:'):
            if not re.search(r'Ref:\s+\w+\.\w+\s+[>-]\s+\w+\.\w+', line_stripped):
                self._add_warning(
                    line_num,
                    "INCOMPLETE_RELATIONSHIP",
                    "Relationship syntax may be incomplete or malformed",
                    line_stripped
                )

    def _validate_overall_structure(self, content: str):
        """Validate overall DBML structure."""

        # Check for basic structure elements
        if 'Table ' not in content:
            self._add_warning(
                0,
                "NO_TABLES",
                "No table definitions found in DBML content",
                ""
            )

        # Check for unmatched braces
        open_braces = content.count('{')
        close_braces = content.count('}')

        if open_braces != close_braces:
            self._add_error(
                0,
                "UNMATCHED_BRACES",
                f"Unmatched braces: {open_braces} opening, {close_braces} closing",
                ""
            )

        # Check for proper project definition
        if 'Project ' not in content:
            self._add_warning(
                0,
                "NO_PROJECT_DEFINITION",
                "No Project definition found - recommended for better organization",
                ""
            )

    def _add_error(self, line_num: int, error_type: str, message: str, line_content: str):
        """Add validation error."""

        error = {
            'line_number': line_num,
            'error_type': error_type,
            'message': message,
            'line_content': line_content,
            'severity': 'ERROR'
        }

        self.validation_errors.append(error)

    def _add_warning(self, line_num: int, warning_type: str, message: str, line_content: str):
        """Add validation warning."""

        warning = {
            'line_number': line_num,
            'warning_type': warning_type,
            'message': message,
            'line_content': line_content,
            'severity': 'WARNING'
        }

        self.validation_warnings.append(warning)

    def fix_syntax_errors(self, dbml_content: str) -> Tuple[str, List[Dict]]:
        """
        Attempt to automatically fix common syntax errors.

        Returns fixed content and list of fixes applied.
        """

        fixes_applied = []
        fixed_content = dbml_content

        # Fix table settings spacing
        old_pattern = r'(Table\s+\w+)\['
        new_pattern = r'\1 ['
        if re.search(old_pattern, fixed_content):
            fixed_content = re.sub(old_pattern, new_pattern, fixed_content)
            fixes_applied.append({
                'fix_type': 'TABLE_SETTINGS_SPACING',
                'description': 'Added required space before table settings brackets'
            })

        # Fix unquoted negative defaults
        old_pattern = r'(default:\s*)(-\d+)(?!\w)'
        new_pattern = r"\1'\2'"
        if re.search(old_pattern, fixed_content):
            fixed_content = re.sub(old_pattern, new_pattern, fixed_content)
            fixes_applied.append({
                'fix_type': 'NEGATIVE_DEFAULTS',
                'description': 'Quoted negative default values'
            })

        # Fix unquoted array types (simple cases)
        old_pattern = r'\b(\w+)\[\](?!\s)'
        new_pattern = r'"\1 []"'
        if re.search(old_pattern, fixed_content):
            fixed_content = re.sub(old_pattern, new_pattern, fixed_content)
            fixes_applied.append({
                'fix_type': 'ARRAY_TYPE_QUOTING',
                'description': 'Quoted array type syntax'
            })

        # Fix multi-word types
        type_replacements = {
            r'double\s+precision': 'float8',
            r'timestamp\s+with\s+time\s+zone': 'timestamptz',
            r'timestamp\s+without\s+time\s+zone': 'timestamp',
            r'character\s+varying': 'varchar'
        }

        for old_type, new_type in type_replacements.items():
            if re.search(old_type, fixed_content, re.IGNORECASE):
                fixed_content = re.sub(old_type, new_type, fixed_content, flags=re.IGNORECASE)
                fixes_applied.append({
                    'fix_type': 'MULTI_WORD_TYPE_REPLACEMENT',
                    'description': f'Replaced multi-word type with alias: {new_type}'
                })

        # Remove line breaks before brackets
        old_pattern = r'\n\s*\['
        new_pattern = ' ['
        if re.search(old_pattern, fixed_content):
            fixed_content = re.sub(old_pattern, new_pattern, fixed_content)
            fixes_applied.append({
                'fix_type': 'BRACKET_LINE_BREAKS',
                'description': 'Removed problematic line breaks before brackets'
            })

        return fixed_content, fixes_applied

    def get_validation_summary(self) -> str:
        """Get human-readable validation summary."""

        if len(self.validation_errors) == 0 and len(self.validation_warnings) == 0:
            return "✅ DBML syntax validation passed - no issues found"

        summary_lines = []

        if self.validation_errors:
            summary_lines.append(f"❌ {len(self.validation_errors)} syntax errors found:")
            for error in self.validation_errors[:5]:  # Show first 5
                line_info = f"Line {error['line_number']}: " if error['line_number'] > 0 else ""
                summary_lines.append(f"   • {line_info}{error['message']}")

            if len(self.validation_errors) > 5:
                summary_lines.append(f"   ... and {len(self.validation_errors) - 5} more errors")

        if self.validation_warnings:
            summary_lines.append(f"⚠️  {len(self.validation_warnings)} syntax warnings found:")
            for warning in self.validation_warnings[:3]:  # Show first 3
                line_info = f"Line {warning['line_number']}: " if warning['line_number'] > 0 else ""
                summary_lines.append(f"   • {line_info}{warning['message']}")

            if len(self.validation_warnings) > 3:
                summary_lines.append(f"   ... and {len(self.validation_warnings) - 3} more warnings")

        return "\n".join(summary_lines)
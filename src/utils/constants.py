"""
Constants and configuration values for the pg2dbml converter.
"""

# Version information
VERSION = "1.0.0"
TOOL_NAME = "pg2dbml"
TOOL_DESCRIPTION = "PostgreSQL to DBML Converter"

# Quality targets
TARGET_SIGMA_LEVEL = 6.0
TARGET_DPMO = 3.4
MINIMUM_ACCEPTABLE_SIGMA = 4.0

# PostgreSQL to DBML type mappings
POSTGRESQL_TO_DBML_TYPES = {
    # Direct mappings (fully supported)
    'integer': 'integer',
    'bigint': 'bigint',
    'smallint': 'integer',
    'boolean': 'boolean',
    'text': 'text',
    'varchar': 'varchar',
    'char': 'char',
    'date': 'date',
    'timestamp': 'timestamp',
    'timestamptz': 'timestamptz',
    'time': 'time',
    'json': 'json',
    'jsonb': 'json',
    'uuid': 'uuid',

    # Partially supported (with transformations)
    'numeric': 'numeric',
    'decimal': 'decimal',
    'real': 'float',
    'float8': 'float',
    'money': 'decimal',
    'interval': 'varchar',
    'serial': 'integer',
    'bigserial': 'bigint',
    'smallserial': 'integer',

    # Unsupported types (fallback to text)
    'inet': 'text',
    'cidr': 'text',
    'macaddr': 'text',
    'macaddr8': 'text',
    'tsvector': 'text',
    'tsquery': 'text',
    'xml': 'text',
    'point': 'text',
    'line': 'text',
    'polygon': 'text',
    'circle': 'text',
    'bytea': 'text',
    'bit': 'text',
    'varbit': 'text',

    # PostgreSQL 14-16 types
    'int4multirange': 'text',
    'int8multirange': 'text',
    'nummultirange': 'text',
    'tsmultirange': 'text',
    'datemultirange': 'text',
}

# Multi-word type aliases
MULTI_WORD_TYPE_ALIASES = {
    'timestamp with time zone': 'timestamptz',
    'timestamp without time zone': 'timestamp',
    'time with time zone': 'timetz',
    'time without time zone': 'time',
    'double precision': 'float8',
    'character varying': 'varchar',
}

# PostgreSQL-specific types that cause semantic loss
SEMANTIC_LOSS_TYPES = {
    'inet', 'cidr', 'macaddr', 'macaddr8',  # Network types
    'tsvector', 'tsquery',  # Text search types
    'xml',  # XML type
    'point', 'line', 'polygon', 'circle',  # Geometric types
    'int4range', 'int8range', 'numrange', 'tsrange', 'daterange',  # Range types
    'int4multirange', 'int8multirange', 'nummultirange', 'tsmultirange', 'datemultirange',  # Multirange types
}

# SQL statements to remove during preprocessing
REMOVE_STATEMENT_PATTERNS = [
    r'^SET\s+',  # Configuration statements
    r'^COMMENT\s+ON\s+',  # Comment statements
    r'^COPY\s+',  # Data import statements
    r'^GRANT\s+',  # Permission statements
    r'^REVOKE\s+',  # Permission statements
    r'OWNER\s+TO\s+',  # Ownership statements
    r'pg_catalog\.setval\(',  # Sequence value statements
]

# Constraint types and their DBML compatibility
CONSTRAINT_COMPATIBILITY = {
    'p': 'SUPPORTED',      # Primary key
    'u': 'SUPPORTED',      # Unique
    'f': 'SUPPORTED',      # Foreign key
    'c': 'UNSUPPORTED',    # Check constraint (parser errors)
    'x': 'UNSUPPORTED',    # Exclude constraint (no DBML equivalent)
    't': 'UNSUPPORTED',    # Trigger constraint (no DBML equivalent)
}

# DBML reserved keywords
DBML_RESERVED_KEYWORDS = {
    'table', 'ref', 'enum', 'project', 'note', 'indexes',
    'and', 'or', 'not', 'null', 'true', 'false',
    'headercolor', 'bgcolor', 'textcolor',
}

# PostgreSQL features that cannot be represented in DBML
UNSUPPORTED_FEATURES = {
    'TABLE_INHERITANCE': 'Table inheritance (INHERITS)',
    'TABLE_PARTITIONING': 'Table partitioning (PARTITION BY/OF)',
    'MATERIALIZED_VIEWS': 'Materialized views',
    'STORED_PROCEDURES': 'Stored procedures and functions',
    'TRIGGERS': 'Triggers and rules',
    'ROW_LEVEL_SECURITY': 'Row-level security',
    'COLUMN_LEVEL_SECURITY': 'Column-level security',
    'FOREIGN_DATA_WRAPPERS': 'Foreign data wrappers',
    'EXTENSIONS': 'PostgreSQL extensions',
    'CUSTOM_TYPES': 'User-defined types',
    'DOMAINS': 'Domain types',
}

# Six Sigma quality thresholds
SIGMA_THRESHOLDS = {
    6.0: 0.9999966,  # 3.4 DPMO
    5.0: 0.999968,   # 32 DPMO
    4.0: 0.99966,    # 340 DPMO
    3.0: 0.9973,     # 2700 DPMO
    2.0: 0.9545,     # 45500 DPMO
    1.0: 0.8413,     # 158700 DPMO
}

# DBML syntax requirements (2024 parser)
DBML_2024_SYNTAX_RULES = {
    'TABLE_SETTINGS_SPACING': 'Table settings require space before bracket: "Table name [settings]"',
    'NEGATIVE_DEFAULTS_QUOTED': 'Negative default values must be quoted: "default: \'-1\'"',
    'ARRAY_TYPES_QUOTED': 'Array types must be quoted: "\"type []\""',
    'FUNCTION_CALLS_BACKTICKS': 'Function calls must use backticks: "`function()`"',
    'NO_LINE_BREAKS_BEFORE_BRACKETS': 'No line breaks allowed before brackets',
    'TRIPLE_QUOTES_FOR_MULTILINE': 'Use triple quotes for multi-line strings',
}

# Error severity levels
ERROR_SEVERITY = {
    'CRITICAL': 3,  # Conversion failure, data loss without warning
    'HIGH': 2,      # Significant feature loss, parser errors
    'MEDIUM': 1,    # Minor feature loss, compatibility issues
    'LOW': 0,       # Cosmetic issues, non-critical warnings
}

# Conversion mode settings (strict mode only)
CONVERSION_MODE = {
    'description': 'High quality conversion with user interaction',
    'allow_silent_failures': False,
    'require_user_decisions': True,
    'target_sigma_level': 4.0,  # Realistic target for lossy PostgreSQL->DBML conversion
}

# File extensions and naming
FILE_EXTENSIONS = {
    'DBML': '.dbml',
    'REPORT': '.report.md',
    'JSON_REPORT': '.report.json',
    'LOG': '.log',
}

# CLI colors and symbols
CLI_SYMBOLS = {
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'INFO': 'ℹ️',
    'PROCESSING': '🔄',
    'QUALITY': '📊',
    'REPORT': '📋',
}

# GitHub issue references for known problems
KNOWN_ISSUES = {
    'ARRAY_TYPES': 'GitHub Issue #46 - Array types cause syntax errors',
    'CHECK_CONSTRAINTS': 'GitHub Issues #68, #484 - CHECK constraints on roadmap since 2020',
    'COMPOSITE_FOREIGN_KEYS': 'GitHub Issues #142, #222 - Composite FK parsing errors',
    'MULTI_WORD_TYPES': 'GitHub Issue #280 - Multi-word type limitations',
    'UUID_FUNCTIONS': 'Community thread #590 - UUID function syntax issues',
    'CASCADE_INVISIBILITY': 'Line 96 Analysis - CASCADE actions not visualized',
}

# Default decision values for automated conversion
DEFAULT_DECISIONS = {
    'ARRAY_TYPE': 'quoted',
    'UNKNOWN_TYPE_FALLBACK': 'text',
    'CHECK_CONSTRAINT_ACTION': 'drop',
    'COMPLEX_INDEX_ACTION': 'simplify',
    'INHERITANCE_ACTION': 'flatten',
    'PARTITIONING_ACTION': 'separate_tables',
}

# Regular expression patterns for SQL parsing
SQL_PATTERNS = {
    'CREATE_TABLE': r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(["\w.]+)',
    'ARRAY_TYPE': r'\b(\w+)\[\]',
    'NEGATIVE_DEFAULT': r'DEFAULT\s+(-\d+)',
    'UUID_FUNCTION': r'gen_random_uuid\(\)',
    'MULTI_WORD_TYPE': r'(timestamp|time)\s+(with|without)\s+time\s+zone',
    'CONSTRAINT_NAME': r'CONSTRAINT\s+(["\w]+)',
    'FOREIGN_KEY': r'FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(["\w.]+)\s*\(([^)]+)\)',
}

# Validation rules for DBML compatibility
VALIDATION_RULES = {
    'MAX_TABLE_NAME_LENGTH': 63,
    'MAX_COLUMN_NAME_LENGTH': 63,
    'MAX_CONSTRAINT_NAME_LENGTH': 63,
    'ALLOWED_IDENTIFIER_PATTERN': r'^[a-zA-Z_][a-zA-Z0-9_]*$',
    'MAX_RELATIONSHIP_COLUMNS': 10,  # Practical limit for diagram clarity
}
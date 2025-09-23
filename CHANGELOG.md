# Changelog

All notable changes to pg2dbml will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-09-24

### Added
- Initial release of pg2dbml
- PostgreSQL SQL dump to DBML conversion
- Support for basic tables, columns, and relationships
- Type mapping from PostgreSQL to DBML
- Array type handling with proper quoting
- Foreign key relationship detection
- Primary key and unique constraint conversion
- Default value preservation
- Quality report generation with metrics
- Silent failure detection
- Comprehensive edge case handling
- CLI interface with progress indicators
- Validation against dbdiagram.io parser

### Known Limitations
- CHECK constraints not supported (DBML limitation)
- Complex PostgreSQL features simplified or dropped
- Views and materialized views skipped
- Triggers and functions not converted
- Partitioned tables converted as separate tables

### Testing
- Tested with 35KB+ edge case files
- Handles 1000+ lines of complex PostgreSQL scenarios
- Never crashes on valid SQL input
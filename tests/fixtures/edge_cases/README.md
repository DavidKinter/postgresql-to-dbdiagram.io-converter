# Edge Case Test Files

These files contain 500+ lines of complex PostgreSQL scenarios designed to test the converter's robustness and error handling. They serve as both regression tests and examples of what the tool can handle.

## Files

- **`postgresql_edge_case.sql`** (1000+ lines): Comprehensive edge cases covering all PostgreSQL features

## What These Test

### Data Types & Features
- All PostgreSQL data types (money, arrays, JSONB, geometric types, etc.)
- Custom domains with CHECK constraints
- ENUM types and ranges
- Complex array types and multi-dimensional arrays

### Schema Complexity
- Reserved SQL keywords as identifiers (`"SELECT"`, `"FROM"`, etc.)
- Complex composite primary keys (5+ columns)
- Self-referential and circular foreign keys
- Table inheritance attempts
- Partitioned tables with complex partition keys
- Unicode and special characters in names

### Advanced Features
- Complex constraints (CHECK, EXCLUDE, DEFERRABLE)
- Generated columns and computed values
- Partial and expression indexes
- Triggers and stored procedures
- Row-level security policies

## Testing Results

✅ **Never crashes** - Handles all 1000+ lines without errors
✅ **Valid DBML output** - Always produces importable files
✅ **Transparent conversion** - Documents everything that gets simplified
✅ **Robust parsing** - Handles malformed and complex SQL

## Usage

```bash
# Test with edge cases
pg2dbml tests/fixtures/edge_cases/postgresql_edge_case.sql -o test_output.dbml --report

# Review what gets converted vs simplified
cat test_output.report.md
```

## For Contributors

These files are excellent for:
- **Testing changes** - Ensure your modifications don't break edge cases
- **Finding limitations** - See what PostgreSQL features need better handling
- **Benchmarking** - Performance testing with complex schemas
- **Learning** - Examples of PostgreSQL features you might not know

Feel free to add more edge cases or suggest improvements based on what you find!
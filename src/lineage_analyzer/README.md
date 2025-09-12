# ETL Lineage Analyzer - SQLGlot Version

This is an enhanced version of the ETL Lineage Analyzer that uses [SQLGlot](https://github.com/tobymao/sqlglot) as the SQL parser instead of regex-based parsing. This provides more accurate and robust SQL parsing, especially for complex Teradata SQL statements.

## Features

- **Accurate SQL Parsing**: Uses SQLGlot's robust SQL parser instead of regex patterns
- **Teradata Support**: Full support for Teradata SQL syntax and dialects
- **Better Error Handling**: More informative error messages and warnings
- **Enhanced Table Extraction**: More precise extraction of table relationships
- **Same Interface**: Drop-in replacement for the original lineage analyzer
- **Comprehensive Reporting**: JSON export, BTEQ export, and detailed lineage reports

## Key Improvements over Regex-based Analyzer

1. **More Accurate Parsing**: SQLGlot provides proper AST-based parsing instead of regex matching
2. **Better Teradata Support**: Handles complex Teradata-specific syntax correctly
3. **Improved Table Detection**: More precise extraction of table names and relationships
4. **Better Error Handling**: Graceful handling of parsing errors with detailed warnings
5. **Extensible**: Easy to add support for additional SQL dialects

## Installation

The SQLGlot-based analyzer requires the `sqlglot` package:

```bash
pip install sqlglot>=27.0.0
```

## Usage

### Command Line Interface

```bash
# Process all SQL files in a folder
python -m lineage_analyzer.lineage sql_files/ output_folder/

# Analyze a single SQL file
python -m lineage_analyzer.lineage my_file.sql output_folder/

# Export to specific JSON file
python -m lineage_analyzer.lineage my_file.sql --export lineage.json

# Show formatted report
python -m lineage_analyzer.lineage my_file.sql --report
```

### Python API

```python
from lineage_analyzer.lineage import ETLLineageAnalyzerSQLGlot

# Initialize analyzer
analyzer = ETLLineageAnalyzerSQLGlot()

# Analyze a single file
lineage_info = analyzer.analyze_script("my_file.sql")

# Print detailed report
analyzer.print_lineage_report(lineage_info)

# Export to JSON
analyzer.export_to_json(lineage_info, "output.json")

# Process entire folder
analyzer.process_folder("sql_files/", "output_folder/")
```

## Output Format

The SQLGlot-based analyzer produces the same output format as the original analyzer, with the addition of a `parser_version` field in JSON exports:

```json
{
  "script_name": "example.sql",
  "parser_version": "SQLGlot",
  "sql_statements": [...],
  "tables": {...},
  "warnings": [...]
}
```

## Supported SQL Operations

- **CREATE VOLATILE TABLE**: Creates temporary tables
- **CREATE VIEW**: Creates database views
- **INSERT**: Inserts data into tables
- **UPDATE**: Updates existing table data
- **DELETE**: Deletes data from tables
- **SELECT**: Queries data from tables (for lineage analysis)
- **MERGE**: Merges data from multiple sources

## Teradata-Specific Features

- **Volatile Tables**: Properly identifies and tracks temporary tables
- **Teradata UPDATE Syntax**: Handles Teradata's unique UPDATE syntax
- **Schema References**: Correctly parses schema.table references
- **BTEQ Commands**: Filters out BTEQ-specific commands during analysis

## Comparison with Regex-based Analyzer

| Feature | Regex-based | SQLGlot-based |
|---------|-------------|---------------|
| SQL Parsing | Regex patterns | AST-based parsing |
| Teradata Support | Limited | Full support |
| Error Handling | Basic | Comprehensive |
| Table Extraction | Pattern-based | Semantic analysis |
| Extensibility | Limited | High |
| Performance | Fast | Moderate |
| Accuracy | Good | Excellent |

## Examples

### Basic Analysis

```python
from lineage_analyzer.lineage import ETLLineageAnalyzerSQLGlot

analyzer = ETLLineageAnalyzerSQLGlot()
lineage_info = analyzer.analyze_script("complex_query.sql")

print(f"Found {len(lineage_info.operations)} operations")
print(f"Source tables: {lineage_info.source_tables}")
print(f"Target tables: {lineage_info.target_tables}")
```

### Batch Processing

```python
# Process entire directory
analyzer.process_folder("sql_scripts/", "lineage_reports/")
```

### Custom Analysis

```python
# Get detailed operation information
for operation in lineage_info.operations:
    print(f"{operation.operation_type}: {operation.target_table}")
    print(f"  Sources: {operation.source_tables}")
    print(f"  Line: {operation.line_number}")
    if operation.is_volatile:
        print("  Type: Volatile Table")
```

## Error Handling

The SQLGlot-based analyzer provides better error handling:

- **Parsing Errors**: Graceful handling of malformed SQL
- **Warning System**: Detailed warnings for potential issues
- **Fallback Parsing**: Attempts to parse even partially valid SQL
- **Error Reporting**: Clear error messages with line numbers

## Performance Considerations

- **Memory Usage**: Slightly higher memory usage due to AST construction
- **Processing Time**: Moderate increase in processing time for complex queries
- **Accuracy**: Significantly improved accuracy in table extraction
- **Scalability**: Better handling of large, complex SQL files

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure SQLGlot is installed: `pip install sqlglot`
2. **Parsing Errors**: Check for unsupported SQL syntax in warnings
3. **Memory Issues**: Process large files individually for better memory management

### Debug Mode

Enable detailed logging to troubleshoot parsing issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

analyzer = ETLLineageAnalyzerSQLGlot()
lineage_info = analyzer.analyze_script("problematic.sql")
```

## Contributing

To extend the SQLGlot-based analyzer:

1. **Add New SQL Operations**: Extend the parser methods in `sqlglot_parser.py`
2. **Support New Dialects**: Add dialect-specific handling in the parser
3. **Enhance Table Extraction**: Improve the `_extract_tables_from_expression` method
4. **Add New Output Formats**: Extend the export methods

## License

Same as the main project - MIT License.

## Dependencies

- `sqlglot>=27.0.0`: SQL parsing and transpilation
- `sqlparse`: SQL formatting and additional parsing support
- `pathlib`: File system operations
- `json`: JSON export functionality
- `datetime`: Timestamp generation

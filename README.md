# SQL Lineage Analyzer

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![CI](https://github.com/harrydu-db/lineage-analyzer/workflows/CI/badge.svg)](https://github.com/harrydu-db/lineage-analyzer/actions)

A Python tool for analyzing ETL shell scripts and SQL files to extract data lineage information, identifying source tables, target tables, and the relationships between them.

## 🚀 Quick Start

```bash
# Install from PyPI (coming soon)
pip install etl-lineage-analyzer

# Or install from source
git clone https://github.com/harrydu-db/lineage-analyzer.git
cd lineage-analyzer
pip install -e .

# Analyze your ETL scripts
lineage-analyzer your_scripts_folder/ output_reports/
```

## Features

- **Batch Processing**: Process all `.sh`, `.ksh`, and `.sql` files in a folder automatically
- **Multiple Output Formats**: Generate JSON, HTML, and text reports
- **Robust SQL Parsing**: Handles complex Teradata SQL with subqueries, aliases, and nested operations
- **Comprehensive Analysis**: Extracts source tables, target tables, volatile tables, and operation details
- **Line Number Tracking**: Provides accurate line numbers for each operation
- **Table Relationship Mapping**: Shows data flow between tables
- **Multiple File Types**: Supports shell scripts (`.sh`, `.ksh`) and direct SQL files (`.sql`)

## Installation

1. Ensure you have Python 3.10+ installed
2. Install required dependencies:
   ```bash
   pip install sqlparse
   ```

## Usage

### Batch Processing (Recommended)

Process all `.sh`, `.ksh`, and `.sql` files in a folder:

```bash
python lineage.py <input_folder> <output_folder>
```

**Example:**
```bash
python lineage.py old/Lotmaster_scripts/ reports/
```

This will:
- Find all `.sh`, `.ksh`, and `.sql` files in `old/Lotmaster_scripts/`
- Generate individual JSON and HTML reports for each file in `reports/`
- Create a processing summary report

### Single File Processing

Analyze a single ETL script or SQL file:

```bash
# Print detailed report to console
python lineage.py BatchTrack.sh
python lineage.py my_etl.sql

# Export to JSON file
python lineage.py BatchTrack.sh --export lineage.json
python lineage.py my_etl.sql --export lineage.json

# Output JSON only (no console report)
python lineage.py BatchTrack.sh --json
python lineage.py my_etl.sql --json
```

## Supported File Types

The analyzer supports three types of files:

1. **Shell Scripts (`.sh`, `.ksh`)**: Files containing `bteq <<EOF ... EOF` blocks
2. **SQL Files (`.sql`)**: Direct SQL files without shell script wrappers
3. **Mixed Content**: Files that may contain both shell script elements and SQL

### File Type Detection

The tool automatically detects the file type and processes accordingly:

- **Shell Scripts**: Extracts SQL from `bteq <<EOF ... EOF` heredoc blocks
- **SQL Files**: Treats the entire content as SQL (removes shell comments and commands)
- **Mixed Files**: Intelligently handles files with both shell and SQL content

## Output Files

### Batch Processing Output

When processing a folder, the tool generates:

1. **JSON Reports** (`*_lineage.json`): Machine-readable format with complete lineage data
2. **HTML Reports** (`*_lineage.html`): Beautiful, formatted reports with styling
3. **Processing Summary** (`processing_summary.txt`): Overview of all processed files

### Report Contents

Each report includes:

- **Summary Statistics**: Total operations, source/target table counts
- **Source Tables**: All tables that provide data to the ETL process
- **Target Tables**: All tables that receive data from the ETL process
- **Volatile Tables**: Temporary tables created during processing
- **Table Relationships**: Data flow mapping between tables
- **Detailed Operations**: CREATE, INSERT, UPDATE operations with line numbers
- **Data Flow Diagram**: Visual representation of the ETL process

## Supported SQL Operations

The analyzer recognizes and extracts lineage from:

- **CREATE VOLATILE TABLE**: Temporary table creation
- **INSERT INTO**: Data insertion operations
- **UPDATE**: Data modification operations
- **SELECT**: Data retrieval (for source table identification)
- **JOIN Operations**: LEFT OUTER JOIN, RIGHT OUTER JOIN, INNER JOIN

## Table Name Extraction

The tool uses multiple methods to extract table names:

1. **sqlparse Library**: Structured SQL parsing
2. **Enhanced Regex Patterns**: Handles complex FROM/JOIN clauses
3. **Subquery Detection**: Extracts tables from nested queries
4. **Alias Handling**: Ignores single-letter aliases (A, B, C, etc.)

### Table Name Validation

The analyzer filters out:
- SQL keywords (SELECT, FROM, WHERE, etc.)
- Single-letter aliases (A, B, C, etc.)
- Names containing SQL expressions
- Invalid table name patterns

## Example Output

### Console Report
```
================================================================================
ETL LINEAGE ANALYSIS REPORT
Script: BatchTrack.sh
================================================================================

📊 SUMMARY:
   • Total Operations: 12
   • Source Tables: 7
   • Target Tables: 8
   • Volatile Tables: 1

🔍 SOURCE TABLES:
   • BATCHTRACK_N
   • BIZT.BIZT_RESP_MSG_LM
   • EDW.LOT_SO_DTL
   • reference.material
   • ...

🎯 TARGET TABLES:
   • BATCHTRACK_N
   • LOTMASTER_BASE_T.LOT_EVT_DTL
   • LOTMASTER_BASE_T.LOT_PICK_DTL
   • ...

🔄 TABLE RELATIONSHIPS:
   • BATCHTRACK_N ← BIZT.BIZT_RESP_MSG_LM_V, BIZT.BIZT_BATCHTRACK_V, LOTMASTER.LOT_SO_DTL
   • LOTMASTER_BASE_T.LOT_EVT_DTL ← BATCHTRACK_N, EDW.LOT_SO_DTL
   • ...
```

### JSON Output Structure
```json
{
  "script_name": "etl_script.sh",
  "summary": {
    "total_operations": 12,
    "source_tables_count": 7,
    "target_tables_count": 8,
    "volatile_tables_count": 1
  },
  "source_tables": ["staging.customer_data", "reference.product_catalog", "analytics.sales_summary", "warehouse.inventory", "external.vendor_data", "temp.processed_orders", "archive.old_records"],
  "target_tables": ["temp.staging_table", "warehouse.final_customer_table", "analytics.monthly_report", "staging.processed_data", "warehouse.aggregated_sales", "temp.intermediate_result", "analytics.dashboard_data", "warehouse.clean_data"],
  "volatile_tables": ["temp.staging_table"],
  "operations": [
    {
      "operation_type": "CREATE_VOLATILE",
      "target_table": "temp.staging_table",
      "source_tables": ["staging.customer_data", "reference.product_catalog", "analytics.sales_summary"],
      "line_number": 6
    },
    {
      "operation_type": "INSERT",
      "target_table": "warehouse.final_customer_table",
      "source_tables": ["temp.staging_table", "warehouse.inventory"],
      "line_number": 15
    }
  ],
  "table_relationships": {
    "temp.staging_table": ["staging.customer_data", "reference.product_catalog", "analytics.sales_summary"],
    "warehouse.final_customer_table": ["temp.staging_table", "warehouse.inventory"],
    "analytics.monthly_report": ["warehouse.final_customer_table", "external.vendor_data"]
  }
}
```

## Error Handling

The tool gracefully handles:
- Files without SQL blocks
- Malformed SQL syntax
- Missing input/output folders
- Permission issues

Failed files are logged in the processing summary with error details.

## Limitations

- Designed primarily for Teradata SQL syntax
- Complex nested subqueries may not be fully parsed
- Some dynamic SQL or stored procedures may not be detected
- Line numbers are approximate for very complex SQL structures

## Troubleshooting

### Common Issues

1. **"No SQL blocks found"**: 
   - For shell scripts: File doesn't contain `bteq <<EOF ... EOF` blocks
   - For SQL files: File may be empty or contain only comments/shell commands
2. **Missing source tables**: Complex SQL with deep nesting may miss some tables
3. **Incorrect line numbers**: Very long SQL statements may have approximate line numbers
4. **SQL file not processed**: Ensure the file has `.sql` extension and contains valid SQL

### Improving Results

- **For Shell Scripts**: Ensure SQL is properly formatted in `bteq` heredoc blocks
- **For SQL Files**: Use clear table aliases (avoid single letters when possible)
- **For Both**: Structure complex queries with proper indentation
- **Mixed Files**: The tool will automatically detect and handle files with both shell and SQL content

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to:

- Report bugs
- Suggest new features
- Submit pull requests
- Set up the development environment

### Development Setup

```bash
# Clone the repository
git clone https://github.com/harrydu-db/lineage-analyzer.git
cd lineage-analyzer

# Set up virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install in development mode
pip install -e .
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Format code
black src/

# Check linting
flake8 src/
```

## 📋 Roadmap

- [ ] Support for PostgreSQL and MySQL syntax
- [ ] Enhanced visualization with interactive diagrams
- [ ] Integration with popular data platforms

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [sqlparse](https://github.com/andialbrecht/sqlparse) for SQL parsing
- Inspired by the need for better ETL documentation and lineage tracking
- Thanks to all contributors and users of this project

## 📞 Support

- 📖 [Documentation](https://github.com/harrydu-db/lineage-analyzer#readme)
- 🐛 [Report a Bug](https://github.com/harrydu-db/lineage-analyzer/issues)
- 💡 [Request a Feature](https://github.com/harrydu-db/lineage-analyzer/issues)
- 💬 [Discussions](https://github.com/harrydu-db/lineage-analyzer/discussions)

---

# ETL Lineage Analyzer

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A powerful Python tool for analyzing ETL shell scripts and SQL files to extract comprehensive data lineage information. Identifies source tables, target tables, and the relationships between them with an interactive HTML viewer for exploring lineage relationships.

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/harrydu-db/lineage-analyzer.git
cd lineage-analyzer

# Install dependencies
pip install sqlparse

# Analyze your ETL scripts
python src/lineage.py your_scripts_folder/ output_reports/

# Open the interactive HTML viewer
open src/lineage_viewer.html
```

## ✨ Features

### Core Analysis
- **📊 Comprehensive Lineage Extraction**: Identifies source tables, target tables, and data flow relationships
- **🔍 Multi-Format Support**: Processes `.sh`, `.ksh`, and `.sql` files automatically
- **⚡ Volatile Table Detection**: Recognizes temporary tables created during ETL processes
- **📝 Line Number Tracking**: Provides accurate line numbers for each operation
- **🔄 Relationship Mapping**: Shows complete data flow between tables

### Interactive HTML Viewer
- **📋 Tables Tab**: Browse table relationships and lineage details
- **🔧 Statements Tab**: View formatted BTEQ SQL statements with syntax highlighting
- **🕸️ Network View**: Interactive network visualization of data flow
- **🔍 Search & Filter**: Find specific tables or statements quickly
- **📊 Network Statistics**: Detailed insights about your data lineage network

### Advanced Network Features
- **🎯 Direct/Indirect Mode**: Toggle between direct connections and full chain visibility
- **🔒 View Locking**: Lock the current view to prevent accidental filter changes
- **🔍 Script Search**: Filter network by specific script names
- **📈 Network Insights**: Statistical analysis of your data lineage network
- **🖱️ Interactive Nodes**: Click tables to highlight connections and view details

## 📁 Supported File Types

### Shell Scripts (`.sh`, `.ksh`)
Extracts SQL from `bteq <<EOF ... EOF` heredoc blocks:
```bash
#!/bin/bash
bteq <<EOF
.LOGON user/password
CREATE VOLATILE TABLE temp.staging AS (
    SELECT * FROM source.table
);
INSERT INTO target.table SELECT * FROM temp.staging;
.QUIT
EOF
```

### SQL Files (`.sql`)
Direct SQL files without shell script wrappers:
```sql
CREATE VOLATILE TABLE temp.staging AS (
    SELECT * FROM source.table
);
INSERT INTO target.table SELECT * FROM temp.staging;
```

### Mixed Content Files
Intelligently handles files with both shell and SQL content.

## 🛠️ Installation

### Prerequisites
- Python 3.10 or higher
- Modern web browser (for HTML viewer)

### Dependencies
```bash
pip install sqlparse
```

## 📖 Usage

### Batch Processing (Recommended)

Process all ETL scripts in a folder:

```bash
python src/lineage.py <input_folder> <output_folder>
```

**Example:**
```bash
python src/lineage.py old/Lotmaster_scripts/ reports/
```

**Output:**
- Individual JSON reports for each script (`*_lineage.json`)
- Cleaned BTEQ SQL files (`*.bteq`)
- Processing summary (`processing_summary.yaml`)
- List of all generated files (`all_lineage.txt`)

### Single File Analysis

```bash
# Show detailed console report
python src/lineage.py BatchTrack.sh --report

# Export to specific JSON file
python src/lineage.py BatchTrack.sh --export lineage.json

# Export to output folder (creates both JSON and .bteq files)
python src/lineage.py BatchTrack.sh output_folder/
```

### Interactive HTML Viewer

1. **Load Data**: Use the file input or drag-and-drop JSON files
2. **Browse Tables**: Click on tables to see their relationships
3. **View Statements**: Browse formatted SQL statements
4. **Explore Network**: Interactive network visualization with search and filtering

**Quick Load**: Add `?json=your_script_lineage.json` to the URL for direct loading.

## 📊 Output Formats

### JSON Reports
Machine-readable format with complete lineage data:
```json
{
  "script_name": "BatchTrack.sh",
  "bteq_statements": [
    "CREATE MULTISET VOLATILE TABLE temp.staging AS\n    (SELECT ...)",
    "INSERT INTO warehouse.final_table\nSELECT ..."
  ],
  "tables": {
    "temp.staging": {
      "source": [
        {
          "name": "source.table",
          "operation": [0]
        }
      ],
      "target": [
        {
          "name": "warehouse.final_table",
          "operation": [1]
        }
      ],
      "is_volatile": true
    }
  },
  "warnings": []
}
```

### BTEQ SQL Files
Cleaned SQL statements without BTEQ control commands:
```sql
CREATE MULTISET VOLATILE TABLE temp.staging AS (
    SELECT 
        customer_id,
        order_date,
        total_amount
    FROM source.customer_orders
    WHERE order_date >= CURRENT_DATE - 30
);

INSERT INTO warehouse.final_customer_summary
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent
FROM temp.staging
GROUP BY customer_id;
```

### Processing Summary
YAML format with processing statistics:
```yaml
# ETL Lineage Analysis Summary
generated_on: 2024-01-15 14:30:25
input_folder: old/Lotmaster_scripts/
output_folder: reports/

statistics:
  total_files_found: 25
  successfully_processed: 23
  failed_to_process: 2
  total_warnings: 5
  files_with_warnings: 3
```

## 🔍 Supported SQL Operations

The analyzer recognizes and extracts lineage from:

- **CREATE VOLATILE TABLE**: Temporary table creation with data
- **INSERT INTO**: Data insertion operations
- **UPDATE**: Data modification operations (Teradata and standard formats)
- **SELECT**: Data retrieval for source table identification
- **JOIN Operations**: LEFT OUTER JOIN, RIGHT OUTER JOIN, INNER JOIN
- **Subqueries**: Nested queries in WHERE, FROM, and SELECT clauses

## 🎯 Table Name Extraction

### Multi-Method Approach
1. **sqlparse Library**: Structured SQL parsing for complex statements
2. **Enhanced Regex Patterns**: Handles complex FROM/JOIN clauses
3. **Subquery Detection**: Extracts tables from nested queries
4. **Alias Handling**: Intelligently filters single-letter aliases

### Validation Rules
The analyzer filters out:
- SQL keywords (SELECT, FROM, WHERE, etc.)
- Single-letter aliases (A, B, C, etc.)
- Names containing SQL expressions
- Invalid table name patterns

## 🌐 Network Visualization Features

### Interactive Controls
- **Direct Mode**: Show only directly connected tables
- **Indirect Mode**: Show all tables connected in the chain
- **View Locking**: Prevent accidental filter changes while moving nodes
- **Script Search**: Filter by specific script names
- **Table Search**: Find specific tables in the network

### Network Statistics
- **Node Count**: Total number of tables
- **Edge Count**: Total number of relationships
- **Connection Density**: Average connections per table
- **Isolated Tables**: Tables with no connections
- **Hub Tables**: Tables with many connections

## 📈 Example Console Report

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
   • Warnings: 0

🔍 SOURCE TABLES:
   • BATCHTRACK_N
   • BIZT.BIZT_RESP_MSG_LM
   • EDW.LOT_SO_DTL
   • reference.material

🎯 TARGET TABLES:
   • BATCHTRACK_N
   • LOTMASTER_BASE_T.LOT_EVT_DTL
   • LOTMASTER_BASE_T.LOT_PICK_DTL

⚡ VOLATILE TABLES (Temporary):
   • temp.staging_table

🔄 TABLE RELATIONSHIPS:
   • BATCHTRACK_N ← BIZT.BIZT_RESP_MSG_LM_V, BIZT.BIZT_BATCHTRACK_V
   • LOTMASTER_BASE_T.LOT_EVT_DTL ← BATCHTRACK_N, EDW.LOT_SO_DTL

📝 DETAILED OPERATIONS:
   1. CREATE_VOLATILE
      Target: temp.staging_table
      Sources: BIZT.BIZT_RESP_MSG_LM_V, BIZT.BIZT_BATCHTRACK_V
      Line: 15

   2. INSERT
      Target: LOTMASTER_BASE_T.LOT_EVT_DTL
      Sources: temp.staging_table, EDW.LOT_SO_DTL
      Line: 45
```

## ⚠️ Error Handling

The tool gracefully handles:
- Files without SQL blocks
- Malformed SQL syntax
- Missing input/output folders
- Permission issues
- BTEQ control statements
- Complex nested subqueries

Failed files are logged in the processing summary with detailed error information.

## 🔧 Advanced Features

### BTEQ Statement Cleaning
- Removes BTEQ control statements (`.LOGON`, `.LOGOFF`, `.SET`, etc.)
- Preserves CREATE VOLATILE TABLE statements
- Maintains SQL formatting and readability

### Network Analysis
- **Connection Analysis**: Identifies data flow patterns
- **Hub Detection**: Finds central tables with many connections
- **Isolation Detection**: Identifies tables with no connections
- **Path Analysis**: Shows data flow paths between tables

### Export Options
- **JSON Export**: Machine-readable lineage data
- **BTEQ SQL Export**: Cleaned SQL statements
- **Console Reports**: Human-readable formatted output
- **Processing Summary**: Batch processing statistics

## 🚧 Limitations

- Designed primarily for Teradata SQL syntax
- Complex dynamic SQL may not be fully parsed
- Stored procedures require manual analysis
- Some very complex nested subqueries may have limited parsing

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [sqlparse](https://github.com/andialbrecht/sqlparse) for SQL parsing
- Network visualization powered by [vis.js](https://visjs.org/)
- Modern UI components and styling
- Made with help from Cursor. 

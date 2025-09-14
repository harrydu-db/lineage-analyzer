# Lineage Analyzer Tools

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A comprehensive toolkit for ETL data lineage analysis with three specialized tools:

1. **SQL Extractor**: Extracts Teradata SQL from shell scripts
2. **Lineage Analyzer**: Analyzes SQL files to extract data lineage using SQLGlot parser
3. **Lineage Viewer**: Interactive React-based web application for visualizing lineage relationships

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/harrydu-db/lineage-analyzer.git
cd lineage-analyzer

# Install dependencies
pip install -r requirements.txt

# Step 1: Extract SQL from shell scripts (if needed)
python src/sql_extractor/extract_sql.py your_shell_scripts/ extracted_sql/

# Step 2: Analyze SQL files for lineage
python src/lineage_analyzer/lineage.py extracted_sql/ lineage_reports/

# Step 3: Start the React-based lineage viewer
cd lineage_viewer_app
python app.py
# Then visit http://localhost:8000
```

## ✨ Features

### 1. SQL Extractor
- **🔍 Shell Script Processing**: Extracts SQL from `.sh` and `.ksh` files
- **📝 BTEQ Command Handling**: Processes `bteq <<EOF ... EOF` heredoc blocks
- **🧹 SQL Cleaning**: Removes BTEQ control statements and comments
- **📁 Batch Processing**: Processes multiple shell scripts at once

### 2. Lineage Analyzer
- **📊 Comprehensive Lineage Extraction**: Identifies source tables, target tables, and data flow relationships
- **🔍 SQL File Support**: Processes `.sql` files using SQLGlot parser for accurate parsing
- **⚡ Volatile Table Detection**: Recognizes temporary tables created during ETL processes
- **📝 Line Number Tracking**: Provides accurate line numbers for each operation
- **🔄 Relationship Mapping**: Shows complete data flow between tables

### 3. Lineage Viewer (React Application)
- **⚛️ Modern React Interface**: Built with React 19 and TypeScript for optimal performance
- **📋 Tables Tab**: Browse table relationships and lineage details with virtualized lists
- **🔧 Statements Tab**: View formatted SQL statements with syntax highlighting
- **🕸️ Network View**: Interactive network visualization powered by vis.js
- **🔍 Advanced Search & Filter**: Find specific tables, statements, or scripts quickly
- **📊 Network Statistics**: Detailed insights about your data lineage network
- **📱 Responsive Design**: Works seamlessly on desktop and mobile devices

#### Advanced Network Features
- **🎯 Direct/Indirect Mode**: Toggle between direct connections and full chain visibility
- **🔒 View Locking**: Lock the current view to prevent accidental filter changes
- **🔍 Script Search**: Filter network by specific script names
- **📈 Network Insights**: Statistical analysis of your data lineage network
- **🖱️ Interactive Nodes**: Click tables to highlight connections and view details
- **⚡ Real-time Updates**: Dynamic data loading and processing

## 📁 Supported File Types

### Shell Scripts (`.sh`, `.ksh`) - SQL Extractor
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

### SQL Files (`.sql`) - Lineage Analyzer
Direct SQL file processing using SQLGlot parser for accurate parsing:
```sql
CREATE VOLATILE TABLE temp.staging AS (
    SELECT * FROM source.table
);
INSERT INTO target.table SELECT * FROM temp.staging;
```

## 🛠️ Installation

### Prerequisites
- Python 3.10 or higher
- Node.js 16+ and npm (for React development)
- Modern web browser (Chrome, Firefox, Safari, Edge)

### Dependencies

#### Python Dependencies
```bash
pip install -r requirements.txt
```

#### React Dependencies
```bash
cd lineage_viewer_app/lineage_viewer_react
npm install
```

## 📖 Usage

### Complete Workflow

#### Step 1: Extract SQL from Shell Scripts (if needed)
```bash
# Extract SQL from shell scripts
python src/sql_extractor/extract_sql.py <shell_scripts_folder> <output_sql_folder>

# Example:
python src/sql_extractor/extract_sql.py Lotmaster_scripts/ extracted_sql/
```

#### Step 2: Analyze SQL Files for Lineage
```bash
# Process all SQL files in a folder
python src/lineage_analyzer/lineage.py <sql_folder> <output_folder>

# Example:
python src/lineage_analyzer/lineage.py extracted_sql/ lineage_reports/

# Single file analysis
python src/lineage_analyzer/lineage.py my_script.sql --report
python src/lineage_analyzer/lineage.py my_script.sql --export lineage.json
```

#### Step 3: Visualize Lineage Data

##### React-based Lineage Viewer (Recommended)
The `lineage_viewer_app` provides a modern React-based web application with enhanced features and better performance.

**Build and run the React app:**
```bash
# Build the React application
cd lineage_viewer_app/lineage_viewer_react
npm run build

# Start the FastAPI server (serves the React app)
cd ..
python app.py
```
Then open http://localhost:8000 in your browser

**Development mode (for React development):**
```bash
# Terminal 1: Start React development server
cd lineage_viewer_app/lineage_viewer_react
npm start

# Terminal 2: Start FastAPI server for data
cd lineage_viewer_app
python app.py
```

**Deploy to Databricks Apps:**
See the detailed deployment instructions in the [lineage_viewer_app README](lineage_viewer_app/README.md) for deploying to Databricks Apps.

### Output Files
- **SQL Extractor**: Clean SQL files (`.sql`)
- **Lineage Analyzer**: 
  - Individual JSON reports (`*_lineage.json`)
  - Formatted SQL files (`*.bteq`)
  - Processing summary (`processing_summary.yaml`)
  - List of all generated files (`all_lineage.txt`)

### Interactive React Viewer

1. **Load Data**: Use the file input or drag-and-drop JSON files, or load from the report folder automatically
2. **Browse Tables**: Click on tables to see their relationships with virtualized lists for better performance
3. **View Statements**: Browse formatted SQL statements with syntax highlighting
4. **Explore Network**: Interactive network visualization with advanced search and filtering capabilities
5. **Script Management**: View and filter data by specific scripts or files

**Features**:
- **Auto-loading**: Automatically loads data from the `report` folder on startup
- **File Upload**: Manual file upload for custom lineage data
- **Real-time Search**: Instant search across tables, statements, and scripts
- **Responsive Design**: Works on desktop and mobile devices

## 📊 Output Formats

### JSON Reports
Machine-readable format with complete lineage data:
```json
{
  "script_name": "my_script.sql",
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

### SQL Files
Formatted SQL statements:
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

### SQLGlot Parser Approach
1. **SQLGlot Library**: Advanced SQL parsing with Teradata dialect support
2. **AST-based Analysis**: Accurate parsing using Abstract Syntax Trees
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
- **Real-time Filtering**: Instant updates as you type

### Network Statistics
- **Node Count**: Total number of tables
- **Edge Count**: Total number of relationships
- **Connection Density**: Average connections per table
- **Isolated Tables**: Tables with no connections
- **Hub Tables**: Tables with many connections
- **Script Analysis**: Statistics per script file

### React-Specific Features
- **Virtualized Lists**: Smooth scrolling through large datasets
- **Component-based Architecture**: Modular, maintainable code structure
- **TypeScript Support**: Type-safe development and better IDE support
- **Modern UI Components**: Clean, responsive interface design

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
- Complex SQL syntax
- Complex nested subqueries

Failed files are logged in the processing summary with detailed error information.

## 🔧 Advanced Features

### SQL Statement Formatting
- Formats SQL statements for better readability
- Preserves CREATE VOLATILE TABLE statements
- Maintains proper SQL formatting and indentation

### Network Analysis
- **Connection Analysis**: Identifies data flow patterns
- **Hub Detection**: Finds central tables with many connections
- **Isolation Detection**: Identifies tables with no connections
- **Path Analysis**: Shows data flow paths between tables

### Export Options
- **JSON Export**: Machine-readable lineage data
- **SQL Export**: Formatted SQL statements
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

- Built with [SQLGlot](https://github.com/tobymao/sqlglot) for SQL parsing
- Network visualization powered by [vis.js](https://visjs.org/)
- React application built with [Create React App](https://create-react-app.dev/)
- Modern UI components and styling
- Made with help from Cursor. 

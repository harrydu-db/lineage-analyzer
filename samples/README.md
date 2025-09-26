# Samples Directory

This directory contains sample files demonstrating how to use the lineage analyzer toolchain. The samples show the complete workflow from source shell scripts with embedded SQL to lineage analysis and visualization.

## Directory Structure

```
samples/
├── README.md                    # This file
├── source/                      # Original shell scripts with embedded SQL
│   ├── sample_sh.sh            # Sample ETL shell script
│   ├── sample_sh.sql           # Extracted SQL from shell script
│   └── PRODUCT_DIM.sql         # Standalone SQL file
├── lineage/                     # Generated lineage analysis results
│   ├── all_lineage.txt         # Summary of all lineage data
│   ├── processing_summary.yaml # Processing statistics
│   ├── sample_sh_sql_lineage.json
│   └── PRODUCT_DIM_sql_lineage.json
└── sample.sql                  # Additional sample SQL file
```

## Usage Workflow

### 1. Source Files (`source/` folder)

The `source/` folder contains the original shell scripts with embedded BTEQ SQL code. These represent typical ETL processes where SQL statements are embedded within shell scripts.

**Example structure:**
- `sample_sh.sh` - A sample ETL shell script with BTEQ SQL blocks
- `sample_sh.sql` - SQL extracted from the shell script
- `PRODUCT_DIM.sql` - SQL file for view definition

### 2. Extract SQL from Shell Scripts

To extract SQL statements from shell scripts containing embedded BTEQ code:

```bash
python src/sql_extractor/sql_extractor.py samples/source/ samples/source
```

This command:
- Processes all `.sh` files in the `samples/source/` directory
- Extracts embedded SQL statements from BTEQ blocks
- Saves the extracted SQL as `.sql` files in the same directory

### 3. Generate Lineage Analysis

To analyze SQL files and generate lineage information:

```bash
python src/lineage_analyzer/lineage.py samples/source samples/lineage
```

This command:
- Analyzes all SQL files (.sql) in the `samples/source/` directory
- Identifies table dependencies and relationships
- Generates detailed lineage JSON files in the `samples/lineage/` directory
- Creates summary files (`all_lineage.txt`, `processing_summary.yaml`)

### 4. View Lineage in the Lineage Viewer

The generated lineage files can be loaded into the lineage viewer application:


For more detailed information about the tools and their configuration options, refer to the main project documentation.

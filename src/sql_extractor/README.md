# SQL Extractor for Shell Files

This Python script extracts SQL statements from shell files that contain BTEQ (Basic Teradata Query) blocks.

## Features

- Extracts SQL statements from BTEQ blocks in shell scripts
- Classifies SQL statements by type (SELECT, INSERT, UPDATE, DELETE, etc.)
- Generates separate files for each statement type
- Creates a combined file with all statements
- Handles comments and BTEQ-specific commands
- Provides detailed logging and error handling

## Usage

### Command Line Usage

```bash
python sql_extractor.py <input_file> <output_folder> [--verbose]
```

**Arguments:**
- `input_file`: Path to the shell file containing BTEQ blocks
- `output_folder`: Path to the output folder for extracted SQL files
- `--verbose` or `-v`: Enable verbose logging

**Example:**
```bash
python sql_extractor.py input/lotmaster/LOT_PROCESS_TRACKING.sh output/extracted_sql --verbose
```

### Programmatic Usage

```python
from sql_extractor import SQLExtractor

# Create extractor
extractor = SQLExtractor("input_file.sh", "output_folder")

# Run extraction
success = extractor.extract()

if success:
    print("Extraction completed successfully!")
else:
    print("Extraction failed!")
```

## Output Files

The script generates a single output file containing all extracted SQL statements:

- `filename.sql` - All SQL statements including BTEQ commands (BT;, ET;)

Each file includes:
- Header with extraction metadata
- Statement type and count information
- Original line numbers for reference
- Properly formatted SQL statements

## Supported SQL Statement Types

- SELECT
- INSERT
- UPDATE
- DELETE
- CREATE
- DROP
- ALTER
- MERGE
- CTE (Common Table Expressions)
- OTHER (unclassified statements)

## Comment and Command Filtering

The script automatically removes comments and filters out BTEQ-specific commands, but preserves important BTEQ commands:

### Comment Removal
- **Block comments**: `/* ... */` are completely removed
- **Line comments**: `--` comments are removed from each line
- Comments are removed before SQL statement extraction

### Command Filtering
- Commands starting with `.` (e.g., `.logon`, `.quit`)
- Shell commands (`sleep`, etc.)
- **Preserved**: `BT;` and `ET;` commands for transaction control

## Error Handling

The script includes comprehensive error handling:

- Input file validation
- Output folder creation
- File reading/writing error handling
- Detailed logging for debugging

## Example Output

For the `LOT_PROCESS_TRACKING.sh` file, the script will generate:

```
output/extracted_sql/
└── LOT_PROCESS_TRACKING.sql
```

This single file contains all SQL statements including BTEQ transaction commands (BT;, ET;).

## Testing

Run the test script to see the extractor in action:

```bash
python test_extractor.py
```

This will process the `LOT_PROCESS_TRACKING.sh` file and generate the output files in the `output/extracted_sql/` directory.

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library)

## License

This script is provided as-is for educational and development purposes. 
#!/usr/bin/env python3
"""
ETL Lineage Analyzer - SQLGlot Version

This script analyzes SQL files to extract data lineage information using SQLGlot
as the SQL parser, providing more accurate parsing than regex-based approaches.

Usage:
    python lineage.py <input_folder> <output_folder>
    python lineage.py <sql_file> [--export output.json]
    
Example:
    python lineage.py sql_files/ reports/
    python lineage.py my_etl.sql --export lineage.json
"""

import sys
import argparse
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path
import json
from datetime import datetime
import logging

# Import the SQLGlot parser
try:
    from .sqlglot_parser import SQLGlotParser, ParsedOperation, ParsedTable
except ImportError:
    # Handle case when script is run directly
    from sqlglot_parser import SQLGlotParser, ParsedOperation, ParsedTable


@dataclass
class TableOperation:
    """Represents a table operation (CREATE, INSERT, UPDATE, etc.)"""

    operation_type: str
    target_table: str
    source_tables: List[str]
    columns: List[str]
    conditions: List[str]
    line_number: int
    sql_statement: str
    is_volatile: bool = False
    is_view: bool = False


@dataclass
class LineageInfo:
    """Complete lineage information for an ETL script"""

    script_name: str
    volatile_tables: List[str]
    source_tables: Set[str]
    target_tables: Set[str]
    operations: List[TableOperation]
    table_relationships: Dict[str, List[str]]
    warnings: List[str]


class ETLLineageAnalyzerSQLGlot:
    """Analyzes SQL files to extract data lineage information using SQLGlot parser"""

    def __init__(self, dialect: str = "teradata") -> None:
        """Initialize the SQLGlot-based lineage analyzer
        
        Args:
            dialect: SQL dialect to use ('teradata', 'spark', 'spark2', etc.)
        """
        self.parser = SQLGlotParser(dialect)
        self.logger = logging.getLogger(__name__)

    def extract_sql_blocks(self, content: str) -> List[str]:
        """Extract SQL blocks from SQL file content"""
        # For SQL files, the entire content is the SQL block
        if content.strip():
            return [content]
        return []


    def extract_operations(self, sql_block: str, warnings: List[str] = None) -> List[TableOperation]:
        """Extract table operations from SQL block using SQLGlot parser"""
        if warnings is None:
            warnings = []
        
        operations = []
        
        # Split into individual statements and track their offsets
        statements_with_offsets = self._split_sql_statements_with_offsets(sql_block)
        
        for statement, offset in statements_with_offsets:
            line_number = self._offset_to_line_number(sql_block, offset)
            
            # Parse using SQLGlot
            parsed_operation = self.parser.parse_sql_statement(statement, line_number)
            
            if parsed_operation:
                # Convert ParsedOperation to TableOperation
                table_operation = self._convert_parsed_operation_to_table_operation(
                    parsed_operation, statement
                )
                if table_operation:
                    operations.append(table_operation)
            else:
                warnings.append(f"Failed to parse SQL statement at line {line_number}")
        
        return operations

    def _split_sql_statements_with_offsets(self, sql_block: str) -> List[Tuple[str, int]]:
        """Split SQL block into statements and return (statement, char_offset) tuples"""
        import re
        
        # Remove comments
        sql_clean = re.sub(r"--.*$", "", sql_block, flags=re.MULTILINE)
        # sql_clean = re.sub(r"/\s*\*.*?\*/", "", sql_clean, flags=re.DOTALL)
        
        statements = []
        current_statement = ""
        paren_count = 0
        start_offset = 0
        
        for i, char in enumerate(sql_clean):
            if not current_statement:
                start_offset = i
            current_statement += char
            
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1
            elif char == ";" and paren_count == 0:
                statements.append((current_statement.strip(), start_offset))
                current_statement = ""
        
        if current_statement.strip():
            statements.append((current_statement.strip(), start_offset))
        
        return statements

    def _offset_to_line_number(self, sql_block: str, offset: int) -> int:
        """Convert a character offset to a line number in the original SQL block"""
        upto = sql_block[:offset]
        return upto.count("\n") + 1

    def _convert_parsed_operation_to_table_operation(
        self, parsed_operation: ParsedOperation, sql_statement: str
    ) -> Optional[TableOperation]:
        """Convert ParsedOperation to TableOperation format"""
        
        # Convert target table
        target_table = ""
        if parsed_operation.target_table:
            target_table = parsed_operation.target_table.full_name.upper()
        
        # Convert source tables
        source_tables = []
        for table in parsed_operation.source_tables:
            if table.full_name:
                source_tables.append(table.full_name.upper())
        
        # Determine operation type with more specific types
        operation_type = parsed_operation.operation_type
        if parsed_operation.is_volatile:
            operation_type = "CREATE_VOLATILE"
        elif parsed_operation.is_view:
            operation_type = "CREATE_VIEW"
        
        return TableOperation(
            operation_type=operation_type,
            target_table=target_table,
            source_tables=source_tables,
            columns=parsed_operation.columns,
            conditions=parsed_operation.conditions,
            line_number=parsed_operation.line_number,
            sql_statement=sql_statement,
            is_volatile=parsed_operation.is_volatile,
            is_view=parsed_operation.is_view
        )

    def analyze_script(self, script_path: str) -> LineageInfo:
        """Analyze a SQL file and extract lineage information using SQLGlot"""
        script_path_obj = Path(script_path)
        warnings = []

        if not script_path_obj.exists():
            raise FileNotFoundError(f"SQL file not found: {script_path_obj}")

        with open(script_path_obj, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # Extract SQL blocks
        sql_blocks = self.extract_sql_blocks(content)

        if not sql_blocks:
            warnings.append("No SQL content found in the file")
            raise ValueError("No SQL content found in the file")

        # Combine all SQL blocks
        combined_sql = "\n".join(sql_blocks)

        # Extract operations using SQLGlot parser
        operations = self.extract_operations(combined_sql, warnings)

        # Separate source and target tables
        source_tables = set()
        target_tables = set()
        volatile_tables = []

        for operation in operations:
            # Filter out empty table names
            if operation.target_table and operation.target_table.strip():
                if operation.operation_type == "CREATE_VOLATILE":
                    volatile_tables.append(operation.target_table)
                    target_tables.add(operation.target_table)
                elif operation.operation_type == "CREATE_VIEW":
                    target_tables.add(operation.target_table)
                elif operation.operation_type in ["INSERT", "UPDATE", "DELETE"]:
                    target_tables.add(operation.target_table)
            
            # Filter out empty source table names
            valid_source_tables = [table for table in operation.source_tables if table and table.strip()]
            source_tables.update(valid_source_tables)

        # Build table relationships
        table_relationships: Dict[str, List[str]] = {}
        for operation in operations:
            # Only process operations with valid target tables
            if operation.target_table and operation.target_table.strip():
                if operation.target_table not in table_relationships:
                    table_relationships[operation.target_table] = []
                # Filter out empty source table names
                valid_source_tables = [table for table in operation.source_tables if table and table.strip()]
                table_relationships[operation.target_table].extend(valid_source_tables)

        return LineageInfo(
            script_name=script_path_obj.name,
            volatile_tables=volatile_tables,
            source_tables=source_tables,
            target_tables=target_tables,
            operations=operations,
            table_relationships=table_relationships,
            warnings=warnings,
        )

    def print_lineage_report(self, lineage_info: LineageInfo) -> None:
        """Print a comprehensive lineage report"""
        print("=" * 80)
        print(f"SQL LINEAGE ANALYSIS REPORT (SQLGlot Version)")
        print(f"File: {lineage_info.script_name}")
        print("=" * 80)

        print("\n📊 SUMMARY:")
        print(f"   • Total Operations: {len(lineage_info.operations)}")
        print(f"   • Source Tables: {len(lineage_info.source_tables)}")
        print(f"   • Target Tables: {len(lineage_info.target_tables)}")
        print(f"   • Volatile Tables: {len(lineage_info.volatile_tables)}")
        print(f"   • Warnings: {len(lineage_info.warnings)}")

        print("\n🔍 SOURCE TABLES:")
        for table in sorted(lineage_info.source_tables):
            print(f"   • {table}")

        print("\n🎯 TARGET TABLES:")
        for table in sorted(lineage_info.target_tables):
            print(f"   • {table}")

        if lineage_info.volatile_tables:
            print("\n⚡ VOLATILE TABLES (Temporary):")
            for table in lineage_info.volatile_tables:
                print(f"   • {table}")

        if lineage_info.warnings:
            print("\n⚠️ WARNINGS:")
            for warning in lineage_info.warnings:
                print(f"   • {warning}")

        print("\n🔄 TABLE RELATIONSHIPS:")
        for target, sources in lineage_info.table_relationships.items():
            if sources:
                print(f"   • {target} ← {', '.join(sources)}")
            else:
                print(f"   • {target} ← (no direct sources)")

        print("\n📝 DETAILED OPERATIONS:")
        for i, operation in enumerate(lineage_info.operations, 1):
            print(f"\n   {i}. {operation.operation_type.upper()}")
            print(f"      Target: {operation.target_table}")
            if operation.source_tables:
                print(f"      Sources: {', '.join(operation.source_tables)}")
            print(f"      Line: {operation.line_number}")
            if operation.is_volatile:
                print(f"      Type: Volatile Table")
            elif operation.is_view:
                print(f"      Type: View")

        print("\n🔄 DATA FLOW:")
        self._print_data_flow(lineage_info)

    def _print_data_flow(self, lineage_info: LineageInfo) -> None:
        """Print the data flow diagram"""
        print("   Source Tables → Processing → Target Tables")
        print("   " + "→".join(["📥"] + ["⚙️"] + ["📤"]))

        # Group by operation type
        operation_groups: Dict[str, List[TableOperation]] = {}
        for op in lineage_info.operations:
            if op.operation_type not in operation_groups:
                operation_groups[op.operation_type] = []
            operation_groups[op.operation_type].append(op)

        for op_type, ops in operation_groups.items():
            print(f"\n   {op_type.upper()} Operations:")
            for op in ops:
                sources = " + ".join(op.source_tables) if op.source_tables else "N/A"
                print(f"      {sources} → {op.target_table}")

    def export_to_json(
        self, lineage_info: LineageInfo, output_file: Optional[str] = None
    ) -> None:
        """Export lineage information to JSON format with data flows for each table"""
        
        # Get all unique tables (filter out empty names)
        all_tables = set()
        all_tables.update(table for table in lineage_info.source_tables if table and table.strip())
        all_tables.update(table for table in lineage_info.target_tables if table and table.strip())
        
        # Collect all unique BTEQ statements
        bteq_statements = []
        statement_to_index = {}
        
        # Process each operation to collect unique statements
        for operation in lineage_info.operations:
            cleaned_statement = operation.sql_statement
            
            # Skip empty statements
            if not cleaned_statement.strip():
                continue
            
            # Format the SQL statement using sqlparse
            import sqlparse
            try:
                formatted_statement = sqlparse.format(
                    cleaned_statement,
                    reindent=True,
                    keyword_case='upper',
                    strip_comments=False,
                    use_space_around_operators=True,
                    indent_width=4
                ).strip()
            except Exception:
                # Fallback to original if formatting fails
                formatted_statement = cleaned_statement
            
            # Add to bteq_statements if not already present
            if formatted_statement not in statement_to_index:
                statement_to_index[formatted_statement] = len(bteq_statements)
                bteq_statements.append(formatted_statement)
        
        # Track which tables are views
        view_tables = set()
        for operation in lineage_info.operations:
            if operation.is_view and operation.target_table:
                view_tables.add(operation.target_table)
        
        # Initialize data structure for each table
        tables_data = {}
        for table in all_tables:
            tables_data[table] = {
                "source": [],
                "target": [],
                "is_volatile": table in lineage_info.volatile_tables
            }
        
        # Process each operation to build the data flows
        for operation in lineage_info.operations:
            operation_type = operation.operation_type
            target_table = operation.target_table
            source_tables = operation.source_tables
            line_number = operation.line_number
            
            cleaned_statement = operation.sql_statement
            
            # Skip operations that result in empty statements
            if not cleaned_statement.strip():
                continue
            
            # Handle operations with empty target tables (e.g., Teradata UPDATE syntax)
            if not target_table or not target_table.strip():
                # Try to infer target table from SQL statement for UPDATE operations
                if operation_type == "UPDATE":
                    # Look for UPDATE table_name pattern in the SQL
                    import re
                    update_match = re.search(r'UPDATE\s+(\w+)\s+FROM\s+([A-Za-z0-9_.]+)', cleaned_statement, re.IGNORECASE)
                    if update_match:
                        # The target table is the second part (after FROM)
                        target_table = update_match.group(2)
                    else:
                        # Try standard UPDATE table_name pattern
                        update_match = re.search(r'UPDATE\s+([A-Za-z0-9_.]+)', cleaned_statement, re.IGNORECASE)
                        if update_match:
                            target_table = update_match.group(1)
                
                # Skip if we still can't determine the target table
                if not target_table or not target_table.strip():
                    continue
                
                # For Teradata UPDATE statements, also extract source tables using regex
                if operation_type == "UPDATE":
                    # Extract tables from FROM clause using regex
                    from_match = re.search(r'FROM\s+([A-Za-z0-9_.]+)', cleaned_statement, re.IGNORECASE)
                    if from_match:
                        from_table = from_match.group(1)
                        if from_table not in source_tables:
                            source_tables.append(from_table)
                    
                    # Extract tables from subqueries in FROM clause
                    subquery_matches = re.findall(r'FROM\s+([A-Za-z0-9_.]+)\s+WHERE', cleaned_statement, re.IGNORECASE)
                    for subquery_table in subquery_matches:
                        if subquery_table not in source_tables:
                            source_tables.append(subquery_table)
            
            # Format the SQL statement using sqlparse
            import sqlparse
            try:
                formatted_statement = sqlparse.format(
                    cleaned_statement,
                    reindent=True,
                    keyword_case='upper',
                    strip_comments=False,
                    use_space_around_operators=True,
                    indent_width=4
                ).strip()
            except Exception:
                # Fallback to original if formatting fails
                formatted_statement = cleaned_statement
            
            # Get the index of the formatted SQL statement
            statement_index = statement_to_index[formatted_statement]
            
            # Add target relationships (this table is a target)
            if target_table in tables_data:
                # Filter out empty source table names
                valid_source_tables = [table for table in source_tables if table and table.strip()]
                for source_table in valid_source_tables:
                    # Check if this source->target relationship already exists
                    existing_source = None
                    for source_rel in tables_data[target_table]["source"]:
                        if source_rel["name"] == source_table:
                            existing_source = source_rel
                            break
                    
                    if existing_source:
                        # Add to existing operations list
                        if statement_index not in existing_source["operation"]:
                            existing_source["operation"].append(statement_index)
                    else:
                        # Create new source relationship
                        tables_data[target_table]["source"].append({
                            "name": source_table,
                            "operation": [statement_index]
                        })
            
            # Add source relationships (source tables have this as a target)
            for source_table in valid_source_tables:
                if source_table in tables_data:
                    # Check if this source->target relationship already exists
                    existing_target = None
                    for target_rel in tables_data[source_table]["target"]:
                        if target_rel["name"] == target_table:
                            existing_target = target_rel
                            break
                    
                    if existing_target:
                        # Add to existing operations list
                        if statement_index not in existing_target["operation"]:
                            existing_target["operation"].append(statement_index)
                    else:
                        # Create new target relationship
                        tables_data[source_table]["target"].append({
                            "name": target_table,
                            "operation": [statement_index]
                        })
        
        # Add warning if no BTEQ statements were found
        if not bteq_statements:
            print(f"⚠️ Warning: No BTEQ statements found in {lineage_info.script_name}. This might indicate:")
            print(f"   - No SQL content detected in the file")
            print(f"   - File contains only comments or empty content")
        
        # Create sorted tables data for consistent JSON output
        sorted_tables_data = {}
        for table_name in sorted(tables_data.keys()):
            sorted_tables_data[table_name] = tables_data[table_name]
        
        data = {
            "script_name": lineage_info.script_name,
            "parser_version": "SQLGlot",
            "bteq_statements": bteq_statements,
            "tables": sorted_tables_data,
            "warnings": lineage_info.warnings
        }

        if output_file:
            # Delete existing file if it exists
            if Path(output_file).exists():
                Path(output_file).unlink()
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            print(f"\n💾 Lineage data exported to: {output_file}")
        else:
            print(json.dumps(data, indent=2))

    def export_to_bteq_sql(self, lineage_info: LineageInfo, output_file: str, original_script_path: str = None) -> None:
        """Export SQL content to a .bteq file"""
        import sqlparse
        
        # Use the provided script path or fall back to the lineage_info script_name
        if original_script_path:
            script_path = Path(original_script_path)
        else:
            script_path = Path(lineage_info.script_name)
            
        if not script_path.exists():
            print(f"⚠️ Warning: Could not find original script file: {script_path}")
            return
        
        # Read the original script
        with open(script_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        
        # For SQL files, use the content directly
        if content.strip():
            # Format the SQL content using sqlparse
            formatted_sql = []
            for statement in sqlparse.split(content):
                formatted = sqlparse.format(
                    statement,
                    reindent=True,
                    keyword_case='upper',
                    strip_comments=False
                )
                formatted_sql.append(formatted.strip())
            pretty_sql = '\n\n'.join(formatted_sql)
            
            # Delete existing file if it exists
            if Path(output_file).exists():
                Path(output_file).unlink()
            
            # Write to .bteq file
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(pretty_sql)
            print(f"💾 SQL exported to: {output_file}")
        else:
            print(f"⚠️ Warning: No SQL content found in {script_path}")

    def process_folder(self, input_folder: str, output_folder: str) -> None:
        """Process all .sql files in the input folder and generate reports in the output folder"""
        input_path = Path(input_folder)
        output_path = Path(output_folder)

        if not input_path.exists():
            raise FileNotFoundError(f"Input folder not found: {input_folder}")

        # Create output folder if it doesn't exist
        output_path.mkdir(parents=True, exist_ok=True)

        # Find all .sql files
        script_files = list(input_path.glob("*.sql"))

        if not script_files:
            print(f"No .sql files found in {input_folder}")
            return

        print(f"Found {len(script_files)} SQL files to process")

        # Process each file
        successful_files = []
        failed_files = []
        total_warnings = 0
        files_with_warnings = 0

        for script_file in script_files:
            try:
                print(f"\nProcessing: {script_file.name}")
                lineage_info = self.analyze_script(str(script_file))

                # Track warnings
                if lineage_info.warnings:
                    total_warnings += len(lineage_info.warnings)
                    files_with_warnings += 1
                    print(f"⚠️ Found {len(lineage_info.warnings)} warnings in {script_file.name}")

                # Generate JSON report with extension included
                json_file = (
                    output_path
                    / f"{script_file.stem}_{script_file.suffix[1:]}_lineage.json"
                )
                self.export_to_json(lineage_info, str(json_file))

                # Generate BTEQ SQL file
                bteq_file = output_path / f"{script_file.stem}.bteq"
                self.export_to_bteq_sql(lineage_info, str(bteq_file), str(script_file))

                successful_files.append(script_file.name)
                print(f"✅ Successfully processed {script_file.name}")

            except Exception as e:
                failed_files.append((script_file.name, str(e)))
                print(f"❌ Failed to process {script_file.name}: {e}")

        # Generate summary report
        summary_file = output_path / "processing_summary.yaml"
        # Delete existing file if it exists
        if summary_file.exists():
            summary_file.unlink()
        with open(summary_file, "w") as f:
            f.write(f"# SQL Lineage Analysis Summary (SQLGlot Version)\n")
            f.write(f"generated_on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"parser: SQLGlot\n")
            f.write(f"input_folder: {input_folder}\n")
            f.write(f"output_folder: {output_folder}\n\n")
            f.write(f"statistics:\n")
            f.write(f"  total_files_found: {len(script_files)}\n")
            f.write(f"  successfully_processed: {len(successful_files)}\n")
            f.write(f"  failed_to_process: {len(failed_files)}\n")
            f.write(f"  total_warnings: {total_warnings}\n")
            f.write(f"  files_with_warnings: {files_with_warnings}\n\n")

            if successful_files:
                f.write("successfully_processed_files:\n")
                for file in successful_files:
                    f.write(f"  - {file}\n")
                f.write("\n")

            if failed_files:
                f.write("failed_files:\n")
                for file, error in failed_files:
                    f.write(f"  - file: {file}\n")
                    f.write(f"    error: {error}\n")

        print(f"\n📊 Processing Summary:")
        print(f"   • Total files: {len(script_files)}")
        print(f"   • Successful: {len(successful_files)}")
        print(f"   • Failed: {len(failed_files)}")
        print(f"   • Total warnings: {total_warnings}")
        print(f"   • Files with warnings: {files_with_warnings}")
        print(f"   • Summary report: {summary_file}")

        # Generate list of JSON files (sorted alphabetically)
        json_files_list = output_path / "all_lineage.txt"
        # Delete existing file if it exists
        if json_files_list.exists():
            json_files_list.unlink()
        with open(json_files_list, "w") as f:
            # Create list of JSON filenames and sort them alphabetically
            json_filenames = []
            for file in successful_files:
                # Extract the base name and create the JSON filename
                base_name = Path(file).stem
                extension = Path(file).suffix[1:]  # Remove the dot
                json_filename = f"{base_name}_{extension}_lineage.json"
                json_filenames.append(json_filename)
            
            # Sort the filenames alphabetically
            json_filenames.sort()
            
            # Write the sorted filenames to the file
            for json_filename in json_filenames:
                f.write(f"{json_filename}\n")
        
        print(f"   • JSON files list: {json_files_list}")


def main() -> None:
    """Main function to run the ETL lineage analyzer with SQLGlot"""
    parser = argparse.ArgumentParser(
        description="Analyze SQL files to extract data lineage information using SQLGlot parser",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all .sql files in a folder for lineage analysis
  python lineage.py sql_files/ reports/
  
  # Analyze a single SQL file with output folder
  python lineage.py my_etl.sql output_folder/
  
  # Analyze a single SQL file with specific export file
  python lineage.py my_etl.sql --export lineage.json
  
  # Analyze Spark SQL files
  python lineage.py spark_files/ reports/ --dialect spark
  
  # Analyze Spark2 SQL files
  python lineage.py spark2_files/ reports/ --dialect spark2
        """,
    )

    parser.add_argument(
        "input",
        help="Input folder containing .sql files OR single SQL file path",
    )

    parser.add_argument(
        "output_folder",
        nargs="?",
        help="Output folder for reports (creates JSON and .bteq files)",
    )

    parser.add_argument(
        "--export", help="Export lineage data to specific JSON file (for single file mode)"
    )

    parser.add_argument(
        "--report", action="store_true", help="Show formatted report instead of JSON output (for single file mode)"
    )

    parser.add_argument(
        "--dialect",
        default="teradata", 
        choices=["teradata", "spark", "spark2"],
        help="SQL dialect to use for parsing (default: teradata)"
    )

    args = parser.parse_args()

    try:
        analyzer = ETLLineageAnalyzerSQLGlot(dialect=args.dialect)
        input_path = Path(args.input)

        # Check if input is a file or folder
        if input_path.is_file():
            # Single file mode
            if not args.output_folder and not args.export and not args.report:
                # Default behavior: output JSON to stdout
                lineage_info = analyzer.analyze_script(args.input)
                analyzer.export_to_json(lineage_info)
            elif args.report:
                # Show formatted report
                lineage_info = analyzer.analyze_script(args.input)
                analyzer.print_lineage_report(lineage_info)
            elif args.export:
                # Export to specified file
                lineage_info = analyzer.analyze_script(args.input)
                analyzer.export_to_json(lineage_info, args.export)
            elif args.output_folder:
                # Export to output folder (creates both JSON and .bteq files)
                output_path = Path(args.output_folder)
                output_path.mkdir(parents=True, exist_ok=True)
                
                lineage_info = analyzer.analyze_script(args.input)
                
                # Generate JSON file
                script_name = Path(args.input).stem
                script_extension = Path(args.input).suffix[1:]  # Remove the dot
                json_file = output_path / f"{script_name}_{script_extension}_lineage.json"
                analyzer.export_to_json(lineage_info, str(json_file))
                
                # Generate BTEQ SQL file
                bteq_file = output_path / f"{script_name}.bteq"
                analyzer.export_to_bteq_sql(lineage_info, str(bteq_file), str(input_path))
                
                print(f"✅ Analysis complete! Files saved to {args.output_folder}/")
                print(f"   • {json_file.name} - Lineage data")
                print(f"   • {bteq_file.name} - Formatted SQL")
            else:
                print("❌ Error: For single file mode, use --export, --report, or specify output folder")
                sys.exit(1)

        elif input_path.is_dir():
            # Folder mode - lineage analysis
            if not args.output_folder:
                print("❌ Error: Output folder is required when processing folders for lineage analysis")
                sys.exit(1)
            analyzer.process_folder(args.input, args.output_folder)

        else:
            print(f"❌ Error: Input path does not exist: {args.input}")
            sys.exit(1)

    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

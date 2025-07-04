#!/usr/bin/env python3
"""
ETL Lineage Analyzer

This script analyzes ETL shell scripts and SQL files to extract data lineage information,
identifying source tables, target tables, and the relationships between them.

Usage:
    python lineage.py <input_folder> <output_folder>
    python lineage.py <script_file> [--export output.json]
    
Example:
    python lineage.py old/Lotmaster_scripts/ reports/
    python lineage.py BatchTrack.sh --export lineage.json
    python lineage.py my_etl.sql --export lineage.json
"""

import re
import sys
import argparse
from typing import Dict, List, Set, Optional
from dataclasses import dataclass
from pathlib import Path
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import json
from datetime import datetime


@dataclass
class TableOperation:
    """Represents a table operation (CREATE, INSERT, UPDATE, etc.)"""

    operation_type: str
    target_table: str
    source_tables: List[str]
    columns: List[str]
    conditions: List[str]
    line_number: int


@dataclass
class LineageInfo:
    """Complete lineage information for an ETL script"""

    script_name: str
    volatile_tables: List[str]
    source_tables: Set[str]
    target_tables: Set[str]
    operations: List[TableOperation]
    table_relationships: Dict[str, List[str]]


class ETLLineageAnalyzer:
    """Analyzes ETL scripts to extract data lineage information"""

    def __init__(self):
        self.sql_keywords = {
            "CREATE",
            "INSERT",
            "UPDATE",
            "SELECT",
            "FROM",
            "JOIN",
            "LEFT",
            "RIGHT",
            "INNER",
            "OUTER",
            "WHERE",
            "AND",
            "OR",
            "IN",
            "EXISTS",
            "UNION",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "GROUP",
            "BY",
            "ORDER",
            "HAVING",
            "DISTINCT",
            "COALESCE",
            "NULL",
            "AS",
            "ON",
            "BT",
            "ET",
            "WITH",
            "DATA",
            "ON",
            "COMMIT",
            "PRESERVE",
            "ROWS",
            "SEL",
            "DISTINCT",
            "CASE",
            "WHEN",
            "CHARACTERS",
            "TRIM",
            "SUBSTR",
            "SUBSTRING",
            "CURRENT_TIMESTAMP",
            "CAST",
            "COALESCE",
        }

        # Common single-letter aliases to ignore
        self.common_aliases = {
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "M",
            "N",
            "O",
            "P",
            "Q",
            "R",
            "S",
            "T",
            "U",
            "V",
            "W",
            "X",
            "Y",
            "Z",
        }

        # Patterns for different SQL operations
        self.patterns = {
            "create_volatile": r"CREATE\s+VOLATILE\s+TABLE\s+(\w+)\s+AS\s*\(",
            "insert": r"INSERT\s+INTO\s+([\w\.]+)\s*\(",
            "update": r"UPDATE\s+([\w\.]+)\s+FROM\s+([\w\.]+)",
            "select_from": r"FROM\s+([\w\.]+)",
            "join": r"JOIN\s+([\w\.]+)",
            "left_join": r"LEFT\s+OUTER\s+JOIN\s+([\w\.]+)",
            "right_join": r"RIGHT\s+OUTER\s+JOIN\s+([\w\.]+)",
            "inner_join": r"INNER\s+JOIN\s+([\w\.]+)",
            "table_alias": r"([\w\.]+)\s+(\w+)",  # table alias pattern
        }

    def is_valid_table_name(self, table_name: str) -> bool:
        """Check if a table name is valid (not an alias or keyword)"""
        if not table_name:
            return False

        table_name = table_name.strip()

        # Skip SQL keywords
        if table_name.upper() in self.sql_keywords:
            return False

        # Skip single-letter aliases
        if len(table_name) == 1 and table_name.upper() in self.common_aliases:
            return False

        # Only filter out if it contains clear SQL expressions
        if any(
            keyword in table_name.upper()
            for keyword in ["SELECT", "CASE", "WHEN", "THEN", "ELSE", "END"]
        ):
            return False

        # Accept names that look like schema.table or table
        if re.match(r"^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)?$", table_name):
            return True

        # Otherwise, be permissive but skip if it contains spaces or commas
        if " " in table_name or "," in table_name:
            return False

        # Must contain at least one letter and be longer than 1 character
        return len(table_name) > 1 and any(c.isalpha() for c in table_name)

    def extract_sql_blocks(self, content: str) -> List[str]:
        """Extract SQL blocks from the shell script or SQL file"""
        sql_blocks = []

        # Check if this looks like a shell script with bteq blocks
        if "bteq" in content.lower() or "<<EOF" in content:
            # Pattern to match bteq heredoc blocks
            bteq_pattern = r"bteq\s*<<EOF\s*(.*?)\s*EOF"
            matches = re.finditer(bteq_pattern, content, re.DOTALL | re.IGNORECASE)

            for match in matches:
                sql_blocks.append(match.group(1))
        else:
            # For .sql files, treat the entire content as SQL
            # Remove shell script elements if present
            sql_content = content

            # Remove common shell script patterns that might be in SQL files
            sql_content = re.sub(
                r"^#!.*$", "", sql_content, flags=re.MULTILINE
            )  # Shebang
            sql_content = re.sub(
                r"^#.*$", "", sql_content, flags=re.MULTILINE
            )  # Shell comments
            sql_content = re.sub(
                r"^\..*$", "", sql_content, flags=re.MULTILINE
            )  # Shell dot commands

            # Clean up empty lines and normalize
            sql_content = re.sub(r"\n\s*\n", "\n", sql_content)  # Remove empty lines
            sql_content = sql_content.strip()

            if sql_content:
                sql_blocks.append(sql_content)

        return sql_blocks

    def extract_table_names(self, sql_block: str) -> Set[str]:
        """Extract table names from SQL block using multiple approaches"""
        tables = set()

        # Remove comments and normalize whitespace
        sql_clean = re.sub(r"--.*$", "", sql_block, flags=re.MULTILINE)
        sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)
        sql_clean = re.sub(r"\s+", " ", sql_clean)  # Normalize whitespace

        # Method 1: Use sqlparse for structured parsing
        try:
            parsed = sqlparse.parse(sql_clean)
            for stmt in parsed:
                for table in self._extract_tables_from_tokenlist(stmt.tokens):
                    if self.is_valid_table_name(table):
                        tables.add(table)
        except Exception as e:
            print(f"Warning: sqlparse failed, using regex fallback: {e}")

        # Method 2: Enhanced regex patterns for FROM/JOIN clauses
        # Handle multiline statements with re.DOTALL
        from_join_patterns = [
            # FROM table alias - more specific pattern
            r"FROM\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s*,|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # FROM table without alias
            r"FROM\s+([a-zA-Z0-9_.]+)(?:\s*,|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # JOIN table alias
            r"JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # JOIN table without alias
            r"JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # LEFT OUTER JOIN table alias
            r"LEFT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # LEFT OUTER JOIN table without alias
            r"LEFT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # RIGHT OUTER JOIN table alias
            r"RIGHT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # RIGHT OUTER JOIN table without alias
            r"RIGHT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # INNER JOIN table alias
            r"INNER\s+JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # INNER JOIN table without alias
            r"INNER\s+JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
        ]

        for pattern in from_join_patterns:
            matches = re.finditer(pattern, sql_clean, re.IGNORECASE | re.DOTALL)
            for match in matches:
                table_name = match.group(1).strip()
                if self.is_valid_table_name(table_name):
                    tables.add(table_name)

        # Method 3: Extract from INSERT/UPDATE statements
        insert_patterns = [
            r"INSERT\s+INTO\s+([a-zA-Z0-9_.]+)(?:\s*\(|\s+SELECT|\s+VALUES)",
            r"UPDATE\s+([a-zA-Z0-9_.]+)(?:\s+\w+)?\s+FROM\s+([a-zA-Z0-9_.]+)(?:\s+\w+)?",
        ]

        for pattern in insert_patterns:
            matches = re.finditer(pattern, sql_clean, re.IGNORECASE | re.DOTALL)
            for match in matches:
                for group_num in range(1, len(match.groups()) + 1):
                    table_name = match.group(group_num).strip()
                    if self.is_valid_table_name(table_name):
                        tables.add(table_name)

        # Method 4: Extract from CREATE VOLATILE TABLE
        create_pattern = r"CREATE\s+VOLATILE\s+TABLE\s+(\w+)\s+AS"
        create_matches = re.finditer(
            create_pattern, sql_clean, re.IGNORECASE | re.DOTALL
        )
        for match in create_matches:
            table_name = match.group(1).strip()
            if self.is_valid_table_name(table_name):
                tables.add(table_name)

        # Method 5: Extract from subqueries in SELECT statements - more specific
        # Look for SELECT ... FROM table patterns within parentheses, but be more careful
        subquery_pattern = r"SELECT\s+.*?\s+FROM\s+([a-zA-Z0-9_.]+)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))"
        subquery_matches = re.finditer(
            subquery_pattern, sql_clean, re.IGNORECASE | re.DOTALL
        )
        for match in subquery_matches:
            table_name = match.group(1).strip()
            if self.is_valid_table_name(table_name):
                tables.add(table_name)

        return tables

    def extract_operations(self, sql_block: str) -> List[TableOperation]:
        """Extract table operations from SQL block with accurate line numbers."""
        operations = []
        # Split into individual statements and track their offsets
        statements_with_offsets = self._split_sql_statements_with_offsets(sql_block)
        for statement, offset in statements_with_offsets:
            line_number = self._offset_to_line_number(sql_block, offset)
            operation = self._parse_sql_statement(statement, line_number)
            if operation:
                operations.append(operation)
        return operations

    def _split_sql_statements_with_offsets(self, sql_block: str):
        """Split SQL block into statements and return (statement, char_offset) tuples."""
        sql_clean = re.sub(r"--.*$", "", sql_block, flags=re.MULTILINE)
        sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)
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
        """Convert a character offset to a line number in the original SQL block."""
        upto = sql_block[:offset]
        return upto.count("\n") + 1

    def _find_statement_line_number(self, statement: str, full_sql: str) -> int:
        """Find the line number where a statement starts in the full SQL block"""
        lines = full_sql.split("\n")
        statement_first_line = statement.strip().split("\n")[0].strip()

        for i, line in enumerate(lines):
            if statement_first_line in line:
                return i + 1

        return 1  # Default to line 1 if not found

    def _parse_sql_statement(
        self, statement: str, line_number: int
    ) -> Optional[TableOperation]:
        """Parse a single SQL statement and extract operation info"""
        statement = statement.strip()

        # CREATE VOLATILE TABLE
        create_match = re.search(
            r"CREATE\s+VOLATILE\s+TABLE\s+(\w+)\s+AS\s*\(",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        if create_match:
            table_name = create_match.group(1)
            source_tables = self._extract_source_tables_from_select(statement)
            return TableOperation(
                operation_type="CREATE_VOLATILE",
                target_table=table_name,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
            )

        # INSERT INTO
        insert_match = re.search(r"INSERT\s+INTO\s+([\w\.]+)", statement, re.IGNORECASE)
        if insert_match:
            table_name = insert_match.group(1)
            source_tables = self._extract_source_tables_from_select(statement)
            return TableOperation(
                operation_type="INSERT",
                target_table=table_name,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
            )

        # UPDATE (Teradata format: UPDATE alias FROM table alias, (subquery) alias)
        update_match = re.search(
            r"UPDATE\s+\w+\s+FROM\s+([\w\.]+)", statement, re.IGNORECASE
        )
        if update_match:
            target_table = update_match.group(1)
            source_tables = self._extract_source_tables_from_update(statement)
            return TableOperation(
                operation_type="UPDATE",
                target_table=target_table,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
            )

        return None

    def _extract_all_source_tables(self, sql: str) -> List[str]:
        """Extract all source tables using sqlparse and fallback regex from FROM/JOIN clauses and subqueries."""
        tables = set()
        # Remove comments
        sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        # Use sqlparse
        try:
            parsed = sqlparse.parse(sql)
            for stmt in parsed:
                for table in self._extract_tables_from_tokenlist(stmt.tokens):
                    if self.is_valid_table_name(table):
                        tables.add(table)
        except Exception as e:
            print(f"Warning: sqlparse failed in source extraction: {e}")

        # Fallback regex for FROM/JOIN - more specific patterns
        from_join_patterns = [
            # FROM table alias - more specific pattern
            r"FROM\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s*,|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # FROM table without alias
            r"FROM\s+([a-zA-Z0-9_.]+)(?:\s*,|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # JOIN table alias
            r"JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # JOIN table without alias
            r"JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # LEFT OUTER JOIN table alias
            r"LEFT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # LEFT OUTER JOIN table without alias
            r"LEFT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # RIGHT OUTER JOIN table alias
            r"RIGHT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # RIGHT OUTER JOIN table without alias
            r"RIGHT\s+OUTER\s+JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # INNER JOIN table alias
            r"INNER\s+JOIN\s+([a-zA-Z0-9_.]+)\s+[a-zA-Z](?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
            # INNER JOIN table without alias
            r"INNER\s+JOIN\s+([a-zA-Z0-9_.]+)(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*\))",
        ]

        for pattern in from_join_patterns:
            for match in re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL):
                table = match.group(1).strip()
                if self.is_valid_table_name(table):
                    tables.add(table)

        # Extract from subqueries in SELECT statements
        subquery_pattern = r"SELECT\s+.*?\s+FROM\s+([a-zA-Z0-9_.]+)"
        for match in re.finditer(subquery_pattern, sql, re.IGNORECASE | re.DOTALL):
            table = match.group(1).strip()
            if self.is_valid_table_name(table):
                tables.add(table)

        return list(tables)

    def _extract_tables_from_tokenlist(self, tokens) -> List[str]:
        """Recursively extract table names from a sqlparse TokenList, handling subqueries."""
        tables = []
        from_seen = False
        for token in tokens:
            if from_seen:
                if self._is_identifier(token):
                    # If the identifier is a subquery, recurse into it
                    if (
                        hasattr(token, "is_group")
                        and token.is_group
                        and any(
                            t.ttype is DML and t.value.upper() == "SELECT"
                            for t in token.tokens
                        )
                    ):
                        tables.extend(self._extract_tables_from_tokenlist(token.tokens))
                    else:
                        name = self._get_identifier_name(token)
                        if name:
                            tables.append(name)
                    from_seen = False
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        if (
                            hasattr(identifier, "is_group")
                            and identifier.is_group
                            and any(
                                t.ttype is DML and t.value.upper() == "SELECT"
                                for t in identifier.tokens
                            )
                        ):
                            tables.extend(
                                self._extract_tables_from_tokenlist(identifier.tokens)
                            )
                        else:
                            name = self._get_identifier_name(identifier)
                            if name:
                                tables.append(name)
                    from_seen = False
                elif token.ttype is Keyword and token.value.upper() in (
                    "SELECT",
                    "WHERE",
                    "GROUP",
                    "ORDER",
                    "HAVING",
                    "UNION",
                    ")",
                ):
                    from_seen = False
            if token.is_group:
                tables.extend(self._extract_tables_from_tokenlist(token.tokens))
            if token.ttype is Keyword and token.value.upper() in (
                "FROM",
                "JOIN",
                "INNER JOIN",
                "LEFT JOIN",
                "LEFT OUTER JOIN",
                "RIGHT JOIN",
                "RIGHT OUTER JOIN",
            ):
                from_seen = True
        return tables

    def _is_identifier(self, token):
        return isinstance(token, Identifier) or (
            token.ttype is None and hasattr(token, "get_real_name")
        )

    def _get_identifier_name(self, identifier):
        # Return the full name including schema if present, skip subqueries
        if (
            hasattr(identifier, "is_group")
            and identifier.is_group
            and any(
                t.ttype is DML and t.value.upper() == "SELECT"
                for t in identifier.tokens
            )
        ):
            return None
        if hasattr(identifier, "get_parent_name") and identifier.get_parent_name():
            return f"{identifier.get_parent_name()}.{identifier.get_real_name()}"
        if hasattr(identifier, "get_real_name") and identifier.get_real_name():
            return identifier.get_real_name()
        return str(identifier)

    def _extract_source_tables_from_select(self, statement: str) -> List[str]:
        """Extract source tables from SELECT statement, recursively."""
        return self._extract_all_source_tables(statement)

    def _extract_source_tables_from_update(self, statement: str) -> List[str]:
        """Extract source tables from UPDATE statement (Teradata format), recursively."""
        return self._extract_all_source_tables(statement)

    def analyze_script(self, script_path: str) -> LineageInfo:
        """Analyze an ETL script and extract lineage information"""
        script_path = Path(script_path)

        if not script_path.exists():
            raise FileNotFoundError(f"Script file not found: {script_path}")

        with open(script_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # Extract SQL blocks
        sql_blocks = self.extract_sql_blocks(content)

        if not sql_blocks:
            raise ValueError("No SQL blocks found in the script")

        # Combine all SQL blocks
        combined_sql = "\n".join(sql_blocks)

        # Extract operations
        operations = self.extract_operations(combined_sql)

        # Separate source and target tables
        source_tables = set()
        target_tables = set()
        volatile_tables = []

        for operation in operations:
            if operation.operation_type == "CREATE_VOLATILE":
                volatile_tables.append(operation.target_table)
                target_tables.add(operation.target_table)
            elif operation.operation_type in ["INSERT", "UPDATE"]:
                target_tables.add(operation.target_table)
                source_tables.update(operation.source_tables)

        # Build table relationships
        table_relationships = {}
        for operation in operations:
            if operation.target_table not in table_relationships:
                table_relationships[operation.target_table] = []
            table_relationships[operation.target_table].extend(operation.source_tables)

        return LineageInfo(
            script_name=script_path.name,
            volatile_tables=volatile_tables,
            source_tables=source_tables,
            target_tables=target_tables,
            operations=operations,
            table_relationships=table_relationships,
        )

    def print_lineage_report(self, lineage_info: LineageInfo):
        """Print a comprehensive lineage report"""
        print("=" * 80)
        print(f"ETL LINEAGE ANALYSIS REPORT")
        print(f"Script: {lineage_info.script_name}")
        print("=" * 80)

        print("\n📊 SUMMARY:")
        print(f"   • Total Operations: {len(lineage_info.operations)}")
        print(f"   • Source Tables: {len(lineage_info.source_tables)}")
        print(f"   • Target Tables: {len(lineage_info.target_tables)}")
        print(f"   • Volatile Tables: {len(lineage_info.volatile_tables)}")

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

        print("\n🔄 DATA FLOW:")
        self._print_data_flow(lineage_info)

    def _print_data_flow(self, lineage_info: LineageInfo):
        """Print the data flow diagram"""
        print("   Source Tables → Processing → Target Tables")
        print("   " + "→".join(["📥"] + ["⚙️"] + ["📤"]))

        # Group by operation type
        operation_groups = {}
        for op in lineage_info.operations:
            if op.operation_type not in operation_groups:
                operation_groups[op.operation_type] = []
            operation_groups[op.operation_type].append(op)

        for op_type, ops in operation_groups.items():
            print(f"\n   {op_type.upper()} Operations:")
            for op in ops:
                sources = " + ".join(op.source_tables) if op.source_tables else "N/A"
                print(f"      {sources} → {op.target_table}")

    def export_to_json(self, lineage_info: LineageInfo, output_file: str = None):
        """Export lineage information to JSON format"""
        data = {
            "script_name": lineage_info.script_name,
            "summary": {
                "total_operations": len(lineage_info.operations),
                "source_tables_count": len(lineage_info.source_tables),
                "target_tables_count": len(lineage_info.target_tables),
                "volatile_tables_count": len(lineage_info.volatile_tables),
            },
            "source_tables": sorted(list(lineage_info.source_tables)),
            "target_tables": sorted(list(lineage_info.target_tables)),
            "volatile_tables": lineage_info.volatile_tables,
            "operations": [
                {
                    "operation_type": op.operation_type,
                    "target_table": op.target_table,
                    "source_tables": op.source_tables,
                    "line_number": op.line_number,
                }
                for op in lineage_info.operations
            ],
            "table_relationships": {
                target: sources
                for target, sources in lineage_info.table_relationships.items()
            },
        }

        if output_file:
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            print(f"\n💾 Lineage data exported to: {output_file}")
        else:
            print(json.dumps(data, indent=2))

    def export_to_html(self, lineage_info: LineageInfo, output_file: str):
        """Export lineage information to HTML format"""
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETL Lineage Report - {lineage_info.script_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        .summary {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }}
        .summary h3 {{ margin-top: 0; color: #495057; }}
        .summary ul {{ list-style: none; padding: 0; }}
        .summary li {{ margin: 5px 0; padding: 5px 10px; background-color: white; border-radius: 3px; }}
        .tables {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }}
        .table-section {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; }}
        .table-section h3 {{ margin-top: 0; color: #495057; }}
        .table-section ul {{ list-style: none; padding: 0; }}
        .table-section li {{ margin: 5px 0; padding: 5px 10px; background-color: white; border-radius: 3px; border-left: 3px solid #007bff; }}
        .operations {{ margin: 20px 0; }}
        .operation {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 10px 0; border-left: 4px solid #28a745; }}
        .operation h4 {{ margin-top: 0; color: #495057; }}
        .relationships {{ margin: 20px 0; }}
        .relationship {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin: 5px 0; border-left: 3px solid #ffc107; }}
        .timestamp {{ text-align: center; color: #6c757d; font-size: 0.9em; margin-top: 30px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>ETL Lineage Analysis Report</h1>
        <p><strong>Script:</strong> {lineage_info.script_name}</p>
        
        <div class="summary">
            <h3>📊 Summary</h3>
            <ul>
                <li>Total Operations: {len(lineage_info.operations)}</li>
                <li>Source Tables: {len(lineage_info.source_tables)}</li>
                <li>Target Tables: {len(lineage_info.target_tables)}</li>
                <li>Volatile Tables: {len(lineage_info.volatile_tables)}</li>
            </ul>
        </div>
        
        <div class="tables">
            <div class="table-section">
                <h3>🔍 Source Tables</h3>
                <ul>
"""

        for table in sorted(lineage_info.source_tables):
            html_content += f"                    <li>{table}</li>\n"

        html_content += """
                </ul>
            </div>
            <div class="table-section">
                <h3>🎯 Target Tables</h3>
                <ul>
"""

        for table in sorted(lineage_info.target_tables):
            html_content += f"                    <li>{table}</li>\n"

        html_content += """
                </ul>
            </div>
        </div>
        
        <div class="relationships">
            <h3>🔄 Table Relationships</h3>
"""

        for target, sources in lineage_info.table_relationships.items():
            if sources:
                html_content += f'            <div class="relationship"><strong>{target}</strong> ← {", ".join(sources)}</div>\n'
            else:
                html_content += f'            <div class="relationship"><strong>{target}</strong> ← (no direct sources)</div>\n'

        html_content += """
        </div>
        
        <div class="operations">
            <h3>📝 Detailed Operations</h3>
"""

        for i, operation in enumerate(lineage_info.operations, 1):
            html_content += f"""
            <div class="operation">
                <h4>{i}. {operation.operation_type.upper()}</h4>
                <p><strong>Target:</strong> {operation.target_table}</p>
"""
            if operation.source_tables:
                html_content += f'                <p><strong>Sources:</strong> {", ".join(operation.source_tables)}</p>\n'
            html_content += f"                <p><strong>Line:</strong> {operation.line_number}</p>\n            </div>\n"

        html_content += f"""
        </div>
        
        <div class="timestamp">
            Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>
"""

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"💾 HTML report exported to: {output_file}")

    def process_folder(self, input_folder: str, output_folder: str):
        """Process all .sh and .ksh files in the input folder and generate reports in the output folder"""
        input_path = Path(input_folder)
        output_path = Path(output_folder)

        if not input_path.exists():
            raise FileNotFoundError(f"Input folder not found: {input_folder}")

        # Create output folder if it doesn't exist
        output_path.mkdir(parents=True, exist_ok=True)

        # Find all .sh and .ksh files
        script_files = (
            list(input_path.glob("*.sh"))
            + list(input_path.glob("*.ksh"))
            + list(input_path.glob("*.sql"))
        )

        if not script_files:
            print(f"No .sh, .ksh, or .sql files found in {input_folder}")
            return

        print(f"Found {len(script_files)} script files to process")

        # Process each file
        successful_files = []
        failed_files = []

        for script_file in script_files:
            try:
                print(f"\nProcessing: {script_file.name}")
                lineage_info = self.analyze_script(script_file)

                # Generate JSON report with extension included
                json_file = (
                    output_path
                    / f"{script_file.stem}_{script_file.suffix[1:]}_lineage.json"
                )
                self.export_to_json(lineage_info, str(json_file))

                # Generate HTML report with extension included
                html_file = (
                    output_path
                    / f"{script_file.stem}_{script_file.suffix[1:]}_lineage.html"
                )
                self.export_to_html(lineage_info, str(html_file))

                successful_files.append(script_file.name)
                print(f"✅ Successfully processed {script_file.name}")

            except Exception as e:
                failed_files.append((script_file.name, str(e)))
                print(f"❌ Failed to process {script_file.name}: {e}")

        # Generate summary report
        summary_file = output_path / "processing_summary.txt"
        with open(summary_file, "w") as f:
            f.write(f"ETL Lineage Analysis Summary\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Input folder: {input_folder}\n")
            f.write(f"Output folder: {output_folder}\n\n")
            f.write(f"Total files found: {len(script_files)}\n")
            f.write(f"Successfully processed: {len(successful_files)}\n")
            f.write(f"Failed to process: {len(failed_files)}\n\n")

            if successful_files:
                f.write("Successfully processed files:\n")
                for file in successful_files:
                    f.write(f"  - {file}\n")
                f.write("\n")

            if failed_files:
                f.write("Failed files:\n")
                for file, error in failed_files:
                    f.write(f"  - {file}: {error}\n")

        print(f"\n📊 Processing Summary:")
        print(f"   • Total files: {len(script_files)}")
        print(f"   • Successful: {len(successful_files)}")
        print(f"   • Failed: {len(failed_files)}")
        print(f"   • Summary report: {summary_file}")


def main():
    """Main function to run the ETL lineage analyzer"""
    parser = argparse.ArgumentParser(
        description="Analyze ETL shell scripts to extract data lineage information",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all .sh, .ksh, and .sql files in a folder
  python lineage.py old/Lotmaster_scripts/ reports/
  
  # Analyze a single file
  python lineage.py BatchTrack.sh --export lineage.json
  python lineage.py my_etl.sql --export lineage.json
  
  # Output lineage data in JSON format only
  python lineage.py BatchTrack.sh --json
        """,
    )

    parser.add_argument(
        "input",
        help="Input folder containing .sh/.ksh/.sql files OR single script file path",
    )

    parser.add_argument(
        "output_folder",
        nargs="?",
        help="Output folder for reports (required when processing folders)",
    )

    parser.add_argument(
        "--export", help="Export lineage data to JSON file (for single file mode)"
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output lineage data in JSON format only (for single file mode)",
    )

    args = parser.parse_args()

    try:
        analyzer = ETLLineageAnalyzer()
        input_path = Path(args.input)

        # Check if input is a file or folder
        if input_path.is_file():
            # Single file mode
            if not args.output_folder and not args.export and not args.json:
                # Default behavior: print report
                lineage_info = analyzer.analyze_script(args.input)
                analyzer.print_lineage_report(lineage_info)
            elif args.json:
                # JSON output only
                lineage_info = analyzer.analyze_script(args.input)
                analyzer.export_to_json(lineage_info)
            elif args.export:
                # Export to specified file
                lineage_info = analyzer.analyze_script(args.input)
                analyzer.export_to_json(lineage_info, args.export)
            else:
                print("❌ Error: For single file mode, use --export or --json flags")
                sys.exit(1)

        elif input_path.is_dir():
            # Folder mode
            if not args.output_folder:
                print("❌ Error: Output folder is required when processing folders")
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

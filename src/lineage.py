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
from typing import Dict, List, Set, Optional, Tuple, Any, cast
from dataclasses import dataclass
from pathlib import Path
import sqlparse  # type: ignore
from sqlparse.sql import IdentifierList, Identifier  # type: ignore
from sqlparse.tokens import Keyword, DML  # type: ignore
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
    sql_statement: str


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


class ETLLineageAnalyzer:
    """Analyzes ETL scripts to extract data lineage information"""

    def __init__(self) -> None:
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
            "create_volatile": r"CREATE\s+(?:MULTISET\s+)?VOLATILE\s+TABLE\s+(\w+)\s+AS\s*\(",
            "create_view": r"CREATE\s+(?:VIEW\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([\w\.]+)\s+AS",
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
            # Extract BTEQ SQL blocks - match the exact EOF delimiter (not part of identifiers)
            bteq_pattern = r"bteq\s*<<EOF\s*(.*?)\s*^EOF\s*$"
            matches = re.finditer(bteq_pattern, content, re.DOTALL | re.IGNORECASE | re.MULTILINE)

            for match in matches:
                sql_block = match.group(1)
                # Remove BTEQ comments (/* ... */) from the SQL block
                sql_block = re.sub(r"/\*.*?\*/", "", sql_block, flags=re.DOTALL)
                # For lineage analysis, use the cleaned SQL block
                # This ensures CREATE VOLATILE TABLE statements are preserved
                if sql_block.strip():
                    sql_blocks.append(sql_block)
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

    def _clean_bteq_sql(self, sql_block: str) -> str:
        """Remove BTEQ control statements and keep only pure SQL"""
        # Remove BTEQ control statements (dot commands)
        # Dot commands always start with a period at the beginning of the line
        # Common dot commands: .LOGON, .LOGOFF, .SET, .IF, .GOTO, .LABEL, .QUIT, .EXPORT, .IMPORT, .RUN, .REPEAT, .SHOW
        # Semicolons are optional for most commands
        bteq_control_patterns = [
            # .LOGON (semicolon optional)
            r"^\s*\.LOGON\s+.*?(?:;|$)",
            # .LOGOFF (semicolon optional)
            r"^\s*\.LOGOFF(?:;|$)",
            # .SET commands (semicolon optional)
            r"^\s*\.SET\s+.*?(?:;|$)",
            # .IF ... THEN GOTO (semicolon optional)
            r"^\s*\.IF\s+.*?THEN\s+GOTO\s+\w+(?:;|$)",
            # .LABEL (semicolon optional)
            r"^\s*\.LABEL\s+\w+(?:;|$)",
            # .EXPORT (semicolon optional)
            r"^\s*\.EXPORT\s+.*?(?:;|$)",
            # .IMPORT (semicolon optional)
            r"^\s*\.IMPORT\s+.*?(?:;|$)",
            # .QUIT (semicolon optional)
            r"^\s*\.QUIT(?:;|$)",
            # .BT/.ET (semicolon optional)
            r"^\s*\.(?:BT|ET)(?:;|$)",
            # .GOTO (semicolon optional)
            r"^\s*\.GOTO\s+\w+(?:;|$)",
            # .SEVERITY (semicolon optional)
            r"^\s*\.SEVERITY\s+\d+(?:;|$)",
            # .ERRORLEVEL (semicolon optional)
            r"^\s*\.ERRORLEVEL\s+.*?(?:;|$)",
            # .ECHOREQ (semicolon optional)
            r"^\s*\.ECHOREQ\s+.*?(?:;|$)",
            # .ERROROUT (semicolon optional)
            r"^\s*\.ERROROUT\s+.*?(?:;|$)",
            # .TITLEDASHES (semicolon optional)
            r"^\s*\.TITLEDASHES\s+.*?(?:;|$)",
            # .WIDTH (semicolon optional)
            r"^\s*\.WIDTH\s+.*?(?:;|$)",
            # .RETRY (semicolon optional)
            r"^\s*\.RETRY\s+.*?(?:;|$)",
            # .RUN FILE = filename (semicolon optional)
            r"^\s*\.RUN\s+FILE\s*=.*?(?:;|$)",
            # .REPEAT (semicolon optional)
            r"^\s*\.REPEAT\s+.*?(?:;|$)",
            # .SHOW (semicolon optional)
            r"^\s*\.SHOW\s+.*?(?:;|$)",
        ]
        
        cleaned_sql = sql_block
        
        # Remove all BTEQ control statements
        for pattern in bteq_control_patterns:
            cleaned_sql = re.sub(pattern, "", cleaned_sql, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        
        # Remove any line that starts with .QUIT (case-insensitive, with or without a semicolon)
        cleaned_sql = re.sub(r"^\s*\.QUIT;?\s*$", "", cleaned_sql, flags=re.IGNORECASE | re.MULTILINE)
        
        # Remove empty lines and normalize whitespace
        cleaned_sql = re.sub(r"\n\s*\n", "\n", cleaned_sql)  # Remove empty lines
        cleaned_sql = re.sub(r"^\s+", "", cleaned_sql, flags=re.MULTILINE)  # Remove leading whitespace
        cleaned_sql = re.sub(r"\s+$", "", cleaned_sql, flags=re.MULTILINE)  # Remove trailing whitespace
        

        
        return cleaned_sql.strip()

    def _clean_bteq_sql_preserve_create(self, sql_block: str) -> str:
        """Remove BTEQ control statements but preserve CREATE VOLATILE TABLE statements"""
        # First, let's preserve CREATE VOLATILE TABLE statements by marking them
        # Replace CREATE VOLATILE TABLE with a special marker
        create_volatile_pattern = r"(CREATE\s+(?:MULTISET\s+)?VOLATILE\s+TABLE\s+\w+\s+AS\s*\(.*?\)WITH\s+DATA.*?ON\s+COMMIT\s+PRESERVE\s+ROWS;)"
        
        # Find all CREATE VOLATILE TABLE statements
        create_statements = re.findall(create_volatile_pattern, sql_block, re.IGNORECASE | re.DOTALL)
        
        # Remove BTEQ control statements but keep CREATE VOLATILE TABLE
        # Dot commands always start with a period at the beginning of the line
        # Common dot commands: .LOGON, .LOGOFF, .SET, .IF, .GOTO, .LABEL, .QUIT, .EXPORT, .IMPORT, .RUN, .REPEAT, .SHOW
        # Semicolons are optional for most commands
        bteq_control_patterns = [
            # .LOGON (semicolon optional)
            r"^\s*\.LOGON\s+.*?(?:;|$)",
            # .LOGOFF (semicolon optional)
            r"^\s*\.LOGOFF(?:;|$)",
            # .SET commands (semicolon optional)
            r"^\s*\.SET\s+.*?(?:;|$)",
            # .IF ... THEN GOTO (semicolon optional)
            r"^\s*\.IF\s+.*?THEN\s+GOTO\s+\w+(?:;|$)",
            # .LABEL (semicolon optional)
            r"^\s*\.LABEL\s+\w+(?:;|$)",
            # .EXPORT (semicolon optional)
            r"^\s*\.EXPORT\s+.*?(?:;|$)",
            # .IMPORT (semicolon optional)
            r"^\s*\.IMPORT\s+.*?(?:;|$)",
            # .QUIT (semicolon optional)
            r"^\s*\.QUIT(?:;|$)",
            # .BT/.ET (semicolon optional)
            r"^\s*\.(?:BT|ET)(?:;|$)",
            # .GOTO (semicolon optional)
            r"^\s*\.GOTO\s+\w+(?:;|$)",
            # .SEVERITY (semicolon optional)
            r"^\s*\.SEVERITY\s+\d+(?:;|$)",
            # .ERRORLEVEL (semicolon optional)
            r"^\s*\.ERRORLEVEL\s+.*?(?:;|$)",
            # .ECHOREQ (semicolon optional)
            r"^\s*\.ECHOREQ\s+.*?(?:;|$)",
            # .ERROROUT (semicolon optional)
            r"^\s*\.ERROROUT\s+.*?(?:;|$)",
            # .TITLEDASHES (semicolon optional)
            r"^\s*\.TITLEDASHES\s+.*?(?:;|$)",
            # .WIDTH (semicolon optional)
            r"^\s*\.WIDTH\s+.*?(?:;|$)",
            # .RETRY (semicolon optional)
            r"^\s*\.RETRY\s+.*?(?:;|$)",
            # .RUN FILE = filename (semicolon optional)
            r"^\s*\.RUN\s+FILE\s*=.*?(?:;|$)",
            # .REPEAT (semicolon optional)
            r"^\s*\.REPEAT\s+.*?(?:;|$)",
            # .SHOW (semicolon optional)
            r"^\s*\.SHOW\s+.*?(?:;|$)",
        ]
        
        cleaned_sql = sql_block
        
        # Remove all BTEQ control statements
        for pattern in bteq_control_patterns:
            cleaned_sql = re.sub(pattern, "", cleaned_sql, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        
        # Remove empty lines and normalize whitespace
        cleaned_sql = re.sub(r"\n\s*\n", "\n", cleaned_sql)  # Remove empty lines
        cleaned_sql = re.sub(r"^\s+", "", cleaned_sql, flags=re.MULTILINE)  # Remove leading whitespace
        cleaned_sql = re.sub(r"\s+$", "", cleaned_sql, flags=re.MULTILINE)  # Remove trailing whitespace
        
        return cleaned_sql.strip()

    def _clean_bteq_sql_for_json(self, sql_block: str) -> str:
        """Remove BTEQ control statements but preserve CREATE VOLATILE TABLE statements for JSON export"""
        # Remove BTEQ comments first
        cleaned_sql = re.sub(r"/\*.*?\*/", "", sql_block, flags=re.DOTALL)
        
        # Split into lines and remove BTEQ control statements
        lines = cleaned_sql.split('\n')
        cleaned_lines = []
        
        for line in lines:
            stripped = line.strip()
            # Skip dot commands (BTEQ control statements) that start with a period
            if stripped.startswith('.'):
                # Check for specific dot commands that should be removed
                dot_command_patterns = [
                    r'^\.LOGON\s+.*?;?$',  # .LOGON (semicolon optional)
                    r'^\.LOGOFF;?$',  # .LOGOFF (semicolon optional)
                    r'^\.SET\s+.*?;?$',  # .SET commands (semicolon optional)
                    r'^\.IF\s+.*?THEN\s+GOTO\s+\w+;?$',  # .IF ... THEN GOTO (semicolon optional)
                    r'^\.LABEL\s+\w+;?$',  # .LABEL (semicolon optional)
                    r'^\.EXPORT\s+.*?;?$',  # .EXPORT (semicolon optional)
                    r'^\.IMPORT\s+.*?;?$',  # .IMPORT (semicolon optional)
                    r'^\.QUIT;?$',  # .QUIT (semicolon optional)
                    r'^\.(?:BT|ET);?$',  # .BT/.ET (semicolon optional)
                    r'^\.GOTO\s+\w+;?$',  # .GOTO (semicolon optional)
                    r'^\.SEVERITY\s+\d+;?$',  # .SEVERITY (semicolon optional)
                    r'^\.ERRORLEVEL\s+.*?;?$',  # .ERRORLEVEL (semicolon optional)
                    r'^\.ECHOREQ\s+.*?;?$',  # .ECHOREQ (semicolon optional)
                    r'^\.ERROROUT\s+.*?;?$',  # .ERROROUT (semicolon optional)
                    r'^\.TITLEDASHES\s+.*?;?$',  # .TITLEDASHES (semicolon optional)
                    r'^\.WIDTH\s+.*?;?$',  # .WIDTH (semicolon optional)
                    r'^\.RETRY\s+.*?;?$',  # .RETRY (semicolon optional)
                    r'^\.RUN\s+FILE\s*=.*?;?$',  # .RUN FILE = filename (semicolon optional)
                    r'^\.REPEAT\s+.*?;?$',  # .REPEAT (semicolon optional)
                    r'^\.SHOW\s+.*?;?$',  # .SHOW (semicolon optional)
                ]
                
                should_skip = False
                for pattern in dot_command_patterns:
                    if re.match(pattern, stripped, re.IGNORECASE):
                        should_skip = True
                        break
                
                if should_skip:
                    continue
            
            cleaned_lines.append(line)
        
        # Join the cleaned lines and normalize whitespace
        cleaned_sql = '\n'.join(cleaned_lines)
        cleaned_sql = re.sub(r'\n\s*\n', '\n', cleaned_sql)  # Remove empty lines
        cleaned_sql = re.sub(r'^\s+', '', cleaned_sql, flags=re.MULTILINE)  # Remove leading whitespace
        cleaned_sql = re.sub(r'\s+$', '', cleaned_sql, flags=re.MULTILINE)  # Remove trailing whitespace
        
        return cleaned_sql.strip()

    def extract_table_names(self, sql_block: str, warnings: List[str] = None) -> Set[str]:
        """Extract table names from SQL block using multiple approaches"""
        if warnings is None:
            warnings = []
        tables = set()

        # Remove comments and normalize whitespace
        sql_clean = re.sub(r"--.*$", "", sql_block, flags=re.MULTILINE)
        # Handle both /* and / * (BTEQ comments with space)
        sql_clean = re.sub(r"/\s*\*.*?\*/", "", sql_clean, flags=re.DOTALL)
        sql_clean = re.sub(r"\s+", " ", sql_clean)  # Normalize whitespace

        # Method 1: Use sqlparse for structured parsing
        try:
            parsed = sqlparse.parse(sql_clean)
            for stmt in parsed:
                for table in self._extract_tables_from_tokenlist(stmt.tokens):
                    if self.is_valid_table_name(table):
                        tables.add(table)
        except Exception as e:
            warning_msg = f"sqlparse failed, using regex fallback: {e}"
            warnings.append(warning_msg)
            print(f"Warning: {warning_msg}")

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
        create_pattern = r"CREATE\s+(?:MULTISET\s+)?VOLATILE\s+TABLE\s+(\w+)\s+AS"
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

    def extract_operations(self, sql_block: str, warnings: List[str] = None) -> List[TableOperation]:
        """Extract table operations from SQL block with accurate line numbers."""
        if warnings is None:
            warnings = []
        operations = []
        # Split into individual statements and track their offsets
        statements_with_offsets = self._split_sql_statements_with_offsets(sql_block)
        for statement, offset in statements_with_offsets:
            line_number = self._offset_to_line_number(sql_block, offset)
            operation = self._parse_sql_statement(statement, line_number, warnings)
            if operation:
                operations.append(operation)
        return operations

    def _split_sql_statements_with_offsets(
        self, sql_block: str
    ) -> List[Tuple[str, int]]:
        """Split SQL block into statements and return (statement, char_offset) tuples."""
        sql_clean = re.sub(r"--.*$", "", sql_block, flags=re.MULTILINE)
        # Handle both /* and / * (BTEQ comments with space)
        sql_clean = re.sub(r"/\s*\*.*?\*/", "", sql_clean, flags=re.DOTALL)
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
        self, statement: str, line_number: int, warnings: List[str] = None
    ) -> Optional[TableOperation]:
        """Parse a single SQL statement and extract operation info"""
        if warnings is None:
            warnings = []
        statement = statement.strip()

        # CREATE VOLATILE TABLE
        create_match = re.search(
            r"CREATE\s+(?:MULTISET\s+)?VOLATILE\s+TABLE\s+(\w+)\s+AS\s*\(",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        if create_match:
            table_name = create_match.group(1)
            source_tables = self._extract_source_tables_from_select(statement, warnings)
            return TableOperation(
                operation_type="CREATE_VOLATILE",
                target_table=table_name,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
                sql_statement=statement,
            )

        # CREATE VIEW
        create_view_match = re.search(
            r"CREATE\s+(?:VIEW\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([\w\.]+)\s+AS",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        if create_view_match:
            view_name = create_view_match.group(1)
            source_tables = self._extract_source_tables_from_select(statement, warnings)
            return TableOperation(
                operation_type="CREATE_VIEW",
                target_table=view_name,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
                sql_statement=statement,
            )

        # INSERT INTO
        insert_match = re.search(r"INSERT\s+INTO\s+([\w\.]+)", statement, re.IGNORECASE)
        if insert_match:
            table_name = insert_match.group(1)
            source_tables = self._extract_source_tables_from_select(statement, warnings)
            return TableOperation(
                operation_type="INSERT",
                target_table=table_name,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
                sql_statement=statement,
            )

        # UPDATE (Teradata format: UPDATE alias FROM table alias, (subquery) alias)
        # Look for the target table in the FROM clause
        update_match = re.search(
            r"UPDATE\s+\w+\s+FROM\s+([\w\.]+)", statement, re.IGNORECASE
        )
        if update_match:
            target_table = update_match.group(1)
            source_tables = self._extract_source_tables_from_update(statement, warnings)
            return TableOperation(
                operation_type="UPDATE",
                target_table=target_table,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
                sql_statement=statement,
            )

        # Standard UPDATE statement (not Teradata format)
        standard_update_match = re.search(
            r"UPDATE\s+([\w\.]+)\s+SET", statement, re.IGNORECASE
        )
        if standard_update_match:
            target_table = standard_update_match.group(1)
            source_tables = self._extract_source_tables_from_update(statement, warnings)
            return TableOperation(
                operation_type="UPDATE",
                target_table=target_table,
                source_tables=source_tables,
                columns=[],
                conditions=[],
                line_number=line_number,
                sql_statement=statement,
            )

        return None

    def _extract_all_source_tables(self, sql: str, warnings: List[str] = None) -> List[str]:
        """Extract all source tables using sqlparse and fallback regex from FROM/JOIN clauses and subqueries."""
        if warnings is None:
            warnings = []
        tables = set()
        # Remove comments
        sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
        # Handle both /* and / * (BTEQ comments with space)
        sql = re.sub(r"/\s*\*.*?\*/", "", sql, flags=re.DOTALL)

        # Use sqlparse
        try:
            parsed = sqlparse.parse(sql)
            for stmt in parsed:
                for table in self._extract_tables_from_tokenlist(stmt.tokens):
                    if self.is_valid_table_name(table):
                        tables.add(table)
        except Exception as e:
            warning_msg = f"sqlparse failed in source extraction: {e}"
            warnings.append(warning_msg)
            print(f"Warning: {warning_msg}")

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

        # Extract from subqueries in WHERE clauses (like the one in the CAMSTAR script)
        where_subquery_pattern = (
            r"WHERE\s+.*?\s+IN\s*\(\s*SELECT\s+.*?\s+FROM\s+([a-zA-Z0-9_.]+)"
        )
        for match in re.finditer(
            where_subquery_pattern, sql, re.IGNORECASE | re.DOTALL
        ):
            table = match.group(1).strip()
            if self.is_valid_table_name(table):
                tables.add(table)

        return list(tables)

    def _extract_tables_from_tokenlist(self, tokens: Any) -> List[str]:
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

    def _is_identifier(self, token: Any) -> bool:
        return isinstance(token, Identifier) or (
            token.ttype is None and hasattr(token, "get_real_name")
        )

    def _get_identifier_name(self, identifier: Any) -> Optional[str]:
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
            return str(identifier.get_real_name())
        if identifier is not None:
            return str(identifier)
        return None

    def _extract_source_tables_from_select(self, statement: str, warnings: List[str] = None) -> List[str]:
        """Extract source tables from SELECT statement, recursively."""
        # For CREATE VOLATILE TABLE, we need to extract tables from the SELECT part
        # Remove the CREATE VOLATILE TABLE part to focus on the SELECT
        select_part = statement
        create_match = re.search(
            r"CREATE\s+(?:MULTISET\s+)?VOLATILE\s+TABLE\s+\w+\s+AS\s*\(\s*(.*)",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        if create_match:
            select_part = create_match.group(1)
            # Remove the closing parenthesis and WITH DATA part
            select_part = re.sub(
                r"\)\s*WITH\s+DATA.*$", "", select_part, flags=re.IGNORECASE | re.DOTALL
            )

        # For CREATE VIEW, we need to extract tables from the SELECT part
        create_view_match = re.search(
            r"CREATE\s+(?:VIEW\s+)?(?:IF\s+NOT\s+EXISTS\s+)?[\w\.]+\s+AS\s*(.*)",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        if create_view_match:
            select_part = create_view_match.group(1)

        return self._extract_all_source_tables(select_part, warnings)

    def _extract_source_tables_from_update(self, statement: str, warnings: List[str] = None) -> List[str]:
        """Extract source tables from UPDATE statement (Teradata format), recursively."""
        # Get all source tables from FROM/JOIN clauses and subqueries
        source_tables = self._extract_all_source_tables(statement, warnings)
        
        # For UPDATE statements, also include the target table as a source table
        # because UPDATE operations read from the target table to update it
        
        # Extract target table name from UPDATE statement
        target_table = None
        
        # Try Teradata format first: UPDATE alias FROM table alias
        teradata_match = re.search(
            r"UPDATE\s+\w+\s+FROM\s+([\w\.]+)", statement, re.IGNORECASE
        )
        if teradata_match:
            target_table = teradata_match.group(1)
        else:
            # Try standard format: UPDATE table SET
            standard_match = re.search(
                r"UPDATE\s+([\w\.]+)\s+SET", statement, re.IGNORECASE
            )
            if standard_match:
                target_table = standard_match.group(1)
        
        # Add target table to source tables if found and valid
        if target_table and self.is_valid_table_name(target_table):
            source_tables.append(target_table)
        
        return source_tables

    def analyze_script(self, script_path: str) -> LineageInfo:
        """Analyze an ETL script and extract lineage information"""
        script_path_obj = Path(script_path)
        warnings = []

        if not script_path_obj.exists():
            raise FileNotFoundError(f"Script file not found: {script_path_obj}")

        with open(script_path_obj, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # Extract SQL blocks
        sql_blocks = self.extract_sql_blocks(content)

        if not sql_blocks:
            warnings.append("No SQL blocks found in the script")
            raise ValueError("No SQL blocks found in the script")

        # Combine all SQL blocks
        combined_sql = "\n".join(sql_blocks)

        # Extract operations
        operations = self.extract_operations(combined_sql, warnings)

        # Separate source and target tables
        source_tables = set()
        target_tables = set()
        volatile_tables = []

        for operation in operations:
            if operation.operation_type == "CREATE_VOLATILE":
                volatile_tables.append(operation.target_table)
                target_tables.add(operation.target_table)
                source_tables.update(operation.source_tables)
            elif operation.operation_type == "CREATE_VIEW":
                target_tables.add(operation.target_table)
                source_tables.update(operation.source_tables)
            elif operation.operation_type in ["INSERT", "UPDATE"]:
                target_tables.add(operation.target_table)
                source_tables.update(operation.source_tables)

        # Build table relationships
        table_relationships: Dict[str, List[str]] = {}
        for operation in operations:
            if operation.target_table not in table_relationships:
                table_relationships[operation.target_table] = []
            table_relationships[operation.target_table].extend(operation.source_tables)

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
        print(f"ETL LINEAGE ANALYSIS REPORT")
        print(f"Script: {lineage_info.script_name}")
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
        
        # Get all unique tables
        all_tables = set()
        all_tables.update(lineage_info.source_tables)
        all_tables.update(lineage_info.target_tables)
        
        # Collect all unique BTEQ statements
        bteq_statements = []
        statement_to_index = {}
        
        # Process each operation to collect unique statements
        for operation in lineage_info.operations:
            # Create cleaned SQL statement (preserving CREATE VOLATILE TABLE)
            cleaned_statement = self._clean_bteq_sql_for_json(operation.sql_statement)
            
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
            
            # Get the cleaned SQL statement
            cleaned_statement = self._clean_bteq_sql_for_json(operation.sql_statement)
            
            # Skip operations that result in empty statements
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
            
            # Get the index of the formatted SQL statement
            statement_index = statement_to_index[formatted_statement]
            
            # Add target relationships (this table is a target)
            if target_table in tables_data:
                for source_table in source_tables:
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
            for source_table in source_tables:
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
            print(f"   - No BTEQ blocks detected in the script")
            print(f"   - All BTEQ statements were filtered out during cleaning")
            print(f"   - Script contains only BTEQ control statements (no SQL)")
        
        # Create sorted tables data for consistent JSON output
        sorted_tables_data = {}
        for table_name in sorted(tables_data.keys()):
            sorted_tables_data[table_name] = tables_data[table_name]
        
        data = {
            "script_name": lineage_info.script_name,
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
        """Export cleaned BTEQ SQL (without control statements) to a .bteq file"""
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
        
        # Extract and clean SQL blocks
        sql_blocks = self.extract_sql_blocks(content)
        if sql_blocks:
            # Combine all cleaned SQL blocks
            cleaned_sql = "\n\n".join(sql_blocks)
            # Remove all dot commands (BTEQ control statements) that start with a period
            cleaned_lines = []
            for line in cleaned_sql.splitlines():
                stripped = line.strip()
                # Skip dot commands (BTEQ control statements) that start with a period
                if stripped.startswith('.'):
                    # Check for specific dot commands that should be removed
                    dot_command_patterns = [
                        r'^\.LOGON\s+.*?;?$',  # .LOGON (semicolon optional)
                        r'^\.LOGOFF;?$',  # .LOGOFF (semicolon optional)
                        r'^\.SET\s+.*?;?$',  # .SET commands (semicolon optional)
                        r'^\.IF\s+.*?THEN\s+GOTO\s+\w+;?$',  # .IF ... THEN GOTO (semicolon optional)
                        r'^\.LABEL\s+\w+;?$',  # .LABEL (semicolon optional)
                        r'^\.EXPORT\s+.*?;?$',  # .EXPORT (semicolon optional)
                        r'^\.IMPORT\s+.*?;?$',  # .IMPORT (semicolon optional)
                        r'^\.QUIT;?$',  # .QUIT (semicolon optional)
                        r'^\.(?:BT|ET);?$',  # .BT/.ET (semicolon optional)
                        r'^\.GOTO\s+\w+;?$',  # .GOTO (semicolon optional)
                        r'^\.SEVERITY\s+\d+;?$',  # .SEVERITY (semicolon optional)
                        r'^\.ERRORLEVEL\s+.*?;?$',  # .ERRORLEVEL (semicolon optional)
                        r'^\.ECHOREQ\s+.*?;?$',  # .ECHOREQ (semicolon optional)
                        r'^\.ERROROUT\s+.*?;?$',  # .ERROROUT (semicolon optional)
                        r'^\.TITLEDASHES\s+.*?;?$',  # .TITLEDASHES (semicolon optional)
                        r'^\.WIDTH\s+.*?;?$',  # .WIDTH (semicolon optional)
                        r'^\.RETRY\s+.*?;?$',  # .RETRY (semicolon optional)
                        r'^\.RUN\s+FILE\s*=.*?;?$',  # .RUN FILE = filename (semicolon optional)
                        r'^\.REPEAT\s+.*?;?$',  # .REPEAT (semicolon optional)
                        r'^\.SHOW\s+.*?;?$',  # .SHOW (semicolon optional)
                    ]
                    
                    should_skip = False
                    for pattern in dot_command_patterns:
                        if re.match(pattern, stripped, re.IGNORECASE):
                            should_skip = True
                            break
                    
                    if should_skip:
                        continue
                

                
                cleaned_lines.append(line)
            final_sql = '\n'.join(cleaned_lines)
            
            # Remove BTEQ comments (/* ... */) from the final SQL
            final_sql = re.sub(r"/\*.*?\*/", "", final_sql, flags=re.DOTALL)
            
            # Format each SQL statement using sqlparse
            formatted_sql = []
            for statement in sqlparse.split(final_sql):
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
            print(f"💾 Cleaned BTEQ SQL exported to: {output_file}")
        else:
            print(f"⚠️ Warning: No SQL blocks found in {script_path}")



    def process_folder(self, input_folder: str, output_folder: str) -> None:
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
            f.write(f"# ETL Lineage Analysis Summary\n")
            f.write(f"generated_on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
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
    """Main function to run the ETL lineage analyzer"""
    parser = argparse.ArgumentParser(
        description="Analyze ETL shell scripts to extract data lineage information",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all .sh, .ksh, and .sql files in a folder
  python lineage.py old/Lotmaster_scripts/ reports/
  
  # Analyze a single file with output folder
  python lineage.py BatchTrack.sh output_folder/
  python lineage.py my_etl.sql output_folder/
  
  # Analyze a single file with specific export file
  python lineage.py BatchTrack.sh --export lineage.json
        """,
    )

    parser.add_argument(
        "input",
        help="Input folder containing .sh/.ksh/.sql files OR single script file path",
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


    args = parser.parse_args()

    try:
        analyzer = ETLLineageAnalyzer()
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
                print(f"   • {bteq_file.name} - Cleaned BTEQ SQL")
            else:
                print("❌ Error: For single file mode, use --export, --report, or specify output folder")
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

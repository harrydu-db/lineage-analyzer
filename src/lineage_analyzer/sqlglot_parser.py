#!/usr/bin/env python3
"""
SQLGlot-based SQL Parser for Teradata SQL

This module provides a robust SQL parser using SQLGlot to replace regex-based parsing.
It specifically handles Teradata SQL syntax and provides better accuracy for complex queries.
"""

import logging
from typing import List, Dict, Set, Optional, Tuple, Any
from dataclasses import dataclass
import sqlglot
from sqlglot import parse_one, parse, Dialect
from sqlglot.expressions import Select, Insert, Update, Delete, Create, Drop, Alter, Merge, CTE
from sqlglot.expressions import Table, Column, Alias, Join, Union, Subquery, Where, And, Or, Not, In, From
from sqlglot.dialects import Teradata, Spark, Spark2


@dataclass
class ParsedTable:
    """Represents a parsed table reference"""
    name: str
    alias: Optional[str] = None
    schema: Optional[str] = None
    is_subquery: bool = False
    
    def __post_init__(self):
        """Initialize ParsedTable with no case normalization"""
        # Preserve original case for both table names and schema names
        # This allows the parser to maintain the exact case as found in SQL
        pass
    
    @property
    def full_name(self) -> str:
        """Get the full table name including schema if present"""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name


@dataclass
class ParsedOperation:
    """Represents a parsed SQL operation"""
    operation_type: str
    target_table: Optional[ParsedTable]
    source_tables: List[ParsedTable]
    columns: List[str]
    conditions: List[str]
    line_number: int
    sql_statement: str
    is_volatile: bool = False
    is_view: bool = False


class SQLGlotParser:
    """SQLGlot-based SQL parser for SQL statements with configurable dialect support"""
    
    def __init__(self, dialect: str = "teradata"):
        """Initialize the SQLGlot parser with specified dialect support
        
        Args:
            dialect: SQL dialect to use ('teradata', 'spark', 'spark2', etc.)
        """
        self.logger = logging.getLogger(__name__)
        self.dialect = self._get_dialect(dialect)
        
        # SQL keywords to filter out
        self.sql_keywords = {
            "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "MERGE",
            "FROM", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "WHERE", "AND", "OR",
            "IN", "EXISTS", "UNION", "CASE", "WHEN", "THEN", "ELSE", "END", "GROUP",
            "BY", "ORDER", "HAVING", "DISTINCT", "COALESCE", "NULL", "AS", "ON",
            "BT", "ET", "WITH", "DATA", "ON", "COMMIT", "PRESERVE", "ROWS", "SEL",
            "CHARACTERS", "TRIM", "SUBSTR", "SUBSTRING", "CURRENT_TIMESTAMP", "CAST"
        }
        
        # Common single-letter aliases to ignore
        self.common_aliases = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    
    def _get_dialect(self, dialect: str) -> Dialect:
        """Get the appropriate SQLGlot dialect object based on the dialect string
        
        Args:
            dialect: Dialect string ('teradata', 'spark', 'spark2', etc.)
            
        Returns:
            SQLGlot Dialect object
            
        Raises:
            ValueError: If the dialect is not supported
        """
        dialect_map = {
            "teradata": Teradata(),
            "spark": Spark(),
            "spark2": Spark2(),
        }
        
        dialect_lower = dialect.lower()
        if dialect_lower not in dialect_map:
            supported_dialects = ", ".join(dialect_map.keys())
            raise ValueError(f"Unsupported dialect '{dialect}'. Supported dialects: {supported_dialects}")
        
        return dialect_map[dialect_lower]
    
    def parse_sql_statement(self, sql: str, line_number: int = 1) -> Optional[ParsedOperation]:
        """
        Parse a single SQL statement using SQLGlot
        
        Args:
            sql: SQL statement to parse
            line_number: Line number in the original file
            
        Returns:
            ParsedOperation object or None if parsing fails
        """
        try:
            # Clean the SQL statement
            cleaned_sql = self._clean_sql(sql)
            if not cleaned_sql.strip():
                return None
            
            # Parse using SQLGlot with specified dialect
            parsed = parse_one(cleaned_sql, dialect=self.dialect)
            if not parsed:
                self.logger.warning(f"Failed to parse SQL at line {line_number}")
                return None
            
            # Determine operation type and extract information
            operation_type = self._get_operation_type(parsed)
            if not operation_type:
                return None
            
            # Extract tables and other information based on operation type
            if operation_type == "SELECT":
                return self._parse_select(parsed, cleaned_sql, line_number)
            elif operation_type == "INSERT":
                return self._parse_insert(parsed, cleaned_sql, line_number)
            elif operation_type == "UPDATE":
                return self._parse_update(parsed, cleaned_sql, line_number)
            elif operation_type == "DELETE":
                return self._parse_delete(parsed, cleaned_sql, line_number)
            elif operation_type == "CREATE":
                return self._parse_create(parsed, cleaned_sql, line_number)
            elif operation_type == "DROP":
                return self._parse_drop(parsed, cleaned_sql, line_number)
            elif operation_type == "ALTER":
                return self._parse_alter(parsed, cleaned_sql, line_number)
            elif operation_type == "MERGE":
                return self._parse_merge(parsed, cleaned_sql, line_number)
            else:
                return self._parse_other(parsed, cleaned_sql, line_number, operation_type)
                
        except Exception as e:
            self.logger.error(f"Error parsing SQL at line {line_number}: {e}")
            return None
    
    def _clean_sql(self, sql: str) -> str:
        """Clean SQL statement by removing comments and extra whitespace"""
        # Remove line comments
        lines = sql.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Find comment position
            comment_pos = line.find('--')
            if comment_pos != -1:
                line = line[:comment_pos].rstrip()
            if line.strip():
                cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def _get_operation_type(self, parsed) -> Optional[str]:
        """Determine the SQL operation type from parsed AST"""
        if isinstance(parsed, Select):
            return "SELECT"
        elif isinstance(parsed, Insert):
            return "INSERT"
        elif isinstance(parsed, Update):
            return "UPDATE"
        elif isinstance(parsed, Delete):
            return "DELETE"
        elif isinstance(parsed, Create):
            return "CREATE"
        elif isinstance(parsed, Drop):
            return "DROP"
        elif isinstance(parsed, Alter):
            return "ALTER"
        elif isinstance(parsed, Merge):
            return "MERGE"
        elif isinstance(parsed, CTE):
            return "CTE"
        else:
            # Check if it's a CTE or other complex statement
            if hasattr(parsed, 'this') and isinstance(parsed.this, Select):
                return "SELECT"
            return "OTHER"
    
    def _parse_select(self, parsed: Select, sql: str, line_number: int) -> ParsedOperation:
        """Parse SELECT statement"""
        source_tables = self._extract_tables_from_select(parsed)
        
        return ParsedOperation(
            operation_type="SELECT",
            target_table=None,  # SELECT doesn't have a target table
            source_tables=source_tables,
            columns=self._extract_columns_from_select(parsed),
            conditions=self._extract_conditions_from_select(parsed),
            line_number=line_number,
            sql_statement=sql
        )
    
    def _parse_insert(self, parsed: Insert, sql: str, line_number: int) -> ParsedOperation:
        """Parse INSERT statement"""
        target_table = self._extract_target_table_from_insert(parsed)
        source_tables = self._extract_tables_from_insert(parsed)
        
        return ParsedOperation(
            operation_type="INSERT",
            target_table=target_table,
            source_tables=source_tables,
            columns=self._extract_columns_from_insert(parsed),
            conditions=[],
            line_number=line_number,
            sql_statement=sql
        )
    
    def _parse_update(self, parsed: Update, sql: str, line_number: int) -> ParsedOperation:
        """Parse UPDATE statement"""
        target_table = self._extract_target_table_from_update(parsed)
        source_tables = self._extract_tables_from_update(parsed)
        
        return ParsedOperation(
            operation_type="UPDATE",
            target_table=target_table,
            source_tables=source_tables,
            columns=self._extract_columns_from_update(parsed),
            conditions=self._extract_conditions_from_update(parsed),
            line_number=line_number,
            sql_statement=sql
        )
    
    def _parse_delete(self, parsed: Delete, sql: str, line_number: int) -> ParsedOperation:
        """Parse DELETE statement"""
        target_table = self._extract_target_table_from_delete(parsed)
        source_tables = self._extract_tables_from_delete(parsed)
        
        return ParsedOperation(
            operation_type="DELETE",
            target_table=target_table,
            source_tables=source_tables,
            columns=[],
            conditions=self._extract_conditions_from_delete(parsed),
            line_number=line_number,
            sql_statement=sql
        )
    
    def _parse_create(self, parsed: Create, sql: str, line_number: int) -> ParsedOperation:
        """Parse CREATE statement (TABLE, VIEW, etc.)"""
        target_table = self._extract_target_table_from_create(parsed)
        source_tables = self._extract_tables_from_create(parsed)
        is_volatile = self._is_volatile_table(parsed)
        is_view = self._is_view(parsed)
        
        return ParsedOperation(
            operation_type="CREATE",
            target_table=target_table,
            source_tables=source_tables,
            columns=self._extract_columns_from_create(parsed),
            conditions=[],
            line_number=line_number,
            sql_statement=sql,
            is_volatile=is_volatile,
            is_view=is_view
        )
    
    def _parse_drop(self, parsed: Drop, sql: str, line_number: int) -> ParsedOperation:
        """Parse DROP statement"""
        target_table = self._extract_target_table_from_drop(parsed)
        
        return ParsedOperation(
            operation_type="DROP",
            target_table=target_table,
            source_tables=[],
            columns=[],
            conditions=[],
            line_number=line_number,
            sql_statement=sql
        )
    
    def _parse_alter(self, parsed: Alter, sql: str, line_number: int) -> ParsedOperation:
        """Parse ALTER statement"""
        target_table = self._extract_target_table_from_alter(parsed)
        
        return ParsedOperation(
            operation_type="ALTER",
            target_table=target_table,
            source_tables=[],
            columns=[],
            conditions=[],
            line_number=line_number,
            sql_statement=sql
        )
    
    def _parse_merge(self, parsed: Merge, sql: str, line_number: int) -> ParsedOperation:
        """Parse MERGE statement"""
        target_table = self._extract_target_table_from_merge(parsed)
        source_tables = self._extract_tables_from_merge(parsed)
        
        return ParsedOperation(
            operation_type="MERGE",
            target_table=target_table,
            source_tables=source_tables,
            columns=[],
            conditions=[],
            line_number=line_number,
            sql_statement=sql
        )
    
    def _parse_other(self, parsed, sql: str, line_number: int, operation_type: str) -> ParsedOperation:
        """Parse other types of statements"""
        return ParsedOperation(
            operation_type=operation_type,
            target_table=None,
            source_tables=[],
            columns=[],
            conditions=[],
            line_number=line_number,
            sql_statement=sql
        )
    
    def _extract_tables_from_select(self, parsed: Select) -> List[ParsedTable]:
        """Extract table references from SELECT statement"""
        tables = []
        
        # Extract FROM clause tables from args
        if hasattr(parsed, 'args') and 'from' in parsed.args:
            from_clause = parsed.args['from']
            if from_clause:
                tables.extend(self._extract_tables_from_expression(from_clause))
        
        # Extract JOIN tables from args
        if hasattr(parsed, 'args') and 'joins' in parsed.args:
            joins = parsed.args['joins']
            if joins:
                if isinstance(joins, list):
                    for join in joins:
                        tables.extend(self._extract_tables_from_expression(join))
                else:
                    tables.extend(self._extract_tables_from_expression(joins))
        
        # Extract tables from WHERE clause
        if hasattr(parsed, 'args') and 'where' in parsed.args:
            where_clause = parsed.args['where']
            if where_clause:
                tables.extend(self._extract_tables_from_expression(where_clause))
        
        return tables
    
    def _extract_tables_from_insert(self, parsed: Insert) -> List[ParsedTable]:
        """Extract table references from INSERT statement"""
        tables = []
        
        # Extract target table
        if parsed.this:
            tables.extend(self._extract_tables_from_expression(parsed.this))
        
        # Extract source tables from SELECT if present
        if parsed.expression:
            # If it's a SELECT statement, extract tables from it
            if isinstance(parsed.expression, Select):
                tables.extend(self._extract_tables_from_select(parsed.expression))
            else:
                # For other expressions, extract tables recursively
                tables.extend(self._extract_tables_from_expression(parsed.expression))
        
        return tables
    
    def _extract_tables_from_update(self, parsed: Update) -> List[ParsedTable]:
        """Extract table references from UPDATE statement"""
        tables = []
        
        # Create alias map for this UPDATE statement
        alias_map = self._build_alias_map(parsed)
        
        # Extract target table
        if hasattr(parsed, 'this') and parsed.this:
            tables.extend(self._extract_tables_from_expression(parsed.this, alias_map))
        
        # Extract FROM clause tables from args
        if hasattr(parsed, 'args') and 'from' in parsed.args:
            from_clause = parsed.args['from']
            if from_clause:
                tables.extend(self._extract_tables_from_expression(from_clause, alias_map))
        
        # Extract JOIN tables from args
        if hasattr(parsed, 'args') and 'joins' in parsed.args:
            joins = parsed.args['joins']
            if joins:
                if isinstance(joins, list):
                    for join in joins:
                        tables.extend(self._extract_tables_from_expression(join, alias_map))
                else:
                    tables.extend(self._extract_tables_from_expression(joins, alias_map))
        
        # Extract tables from WHERE clause
        if hasattr(parsed, 'args') and 'where' in parsed.args:
            where_clause = parsed.args['where']
            if where_clause:
                tables.extend(self._extract_tables_from_expression(where_clause, alias_map))
        
        return tables
    
    def _extract_tables_from_delete(self, parsed: Delete) -> List[ParsedTable]:
        """Extract table references from DELETE statement"""
        tables = []
        
        # Extract target table
        if hasattr(parsed, 'this') and parsed.this:
            tables.extend(self._extract_tables_from_expression(parsed.this))
        
        # Extract FROM clause tables from args
        if hasattr(parsed, 'args') and 'from' in parsed.args:
            from_clause = parsed.args['from']
            if from_clause:
                tables.extend(self._extract_tables_from_expression(from_clause))
        
        # Extract JOIN tables from args
        if hasattr(parsed, 'args') and 'joins' in parsed.args:
            joins = parsed.args['joins']
            if joins:
                if isinstance(joins, list):
                    for join in joins:
                        tables.extend(self._extract_tables_from_expression(join))
                else:
                    tables.extend(self._extract_tables_from_expression(joins))
        
        # Extract tables from WHERE clause (including subqueries)
        if hasattr(parsed, 'args') and 'where' in parsed.args:
            where_clause = parsed.args['where']
            if where_clause:
                tables.extend(self._extract_tables_from_expression(where_clause))
        
        return tables
    
    def _extract_tables_from_create(self, parsed: Create) -> List[ParsedTable]:
        """Extract table references from CREATE statement"""
        tables = []
        
        # For CREATE TABLE AS SELECT, extract tables from the SELECT
        if parsed.expression:
            # If it's a Subquery, extract the Select statement from it
            if hasattr(parsed.expression, 'this') and hasattr(parsed.expression.this, 'args'):
                # It's a Subquery containing a Select
                select_stmt = parsed.expression.this
                tables.extend(self._extract_tables_from_select(select_stmt))
            else:
                # Direct expression
                tables.extend(self._extract_tables_from_expression(parsed.expression))
        
        return tables
    
    def _extract_tables_from_merge(self, parsed: Merge) -> List[ParsedTable]:
        """Extract table references from MERGE statement"""
        tables = []
        
        # Extract target table
        if parsed.this:
            tables.extend(self._extract_tables_from_expression(parsed.this))
        
        # Extract source table
        if parsed.using:
            tables.extend(self._extract_tables_from_expression(parsed.using))
        
        return tables
    
    def _extract_tables_from_expression(self, expression, alias_map: Dict[str, str] = None) -> List[ParsedTable]:
        """Recursively extract table references from any expression"""
        tables = []
        
        if isinstance(expression, Table):
            table = self._create_parsed_table_from_table(expression)
            if table:
                tables.append(table)
            # Also extract tables from joins
            if hasattr(expression, 'args') and 'joins' in expression.args:
                joins = expression.args['joins']
                if joins:
                    for join in joins:
                        tables.extend(self._extract_tables_from_expression(join, alias_map))
        elif isinstance(expression, Alias):
            # Handle table aliases
            if isinstance(expression.this, Table):
                table = self._create_parsed_table_from_table(expression.this)
                if table:
                    table.alias = expression.alias
                    tables.append(table)
            else:
                # Recursively extract from the aliased expression
                tables.extend(self._extract_tables_from_expression(expression.this, alias_map))
        elif isinstance(expression, Subquery):
            # Handle subqueries
            tables.extend(self._extract_tables_from_expression(expression.this, alias_map))
        elif isinstance(expression, Select):
            # Handle SELECT statements
            tables.extend(self._extract_tables_from_select(expression))
        elif isinstance(expression, Union):
            # Handle UNION statements
            tables.extend(self._extract_tables_from_expression(expression.left, alias_map))
            tables.extend(self._extract_tables_from_expression(expression.right, alias_map))
        elif isinstance(expression, Where):
            # Handle WHERE clauses
            if hasattr(expression, 'this') and expression.this:
                tables.extend(self._extract_tables_from_expression(expression.this, alias_map))
        elif isinstance(expression, (And, Or)):
            # Handle AND/OR expressions
            if hasattr(expression, 'this') and expression.this:
                tables.extend(self._extract_tables_from_expression(expression.this, alias_map))
            if hasattr(expression, 'expressions'):
                for expr in expression.expressions:
                    tables.extend(self._extract_tables_from_expression(expr, alias_map))
        elif isinstance(expression, Not):
            # Handle NOT expressions
            if hasattr(expression, 'this') and expression.this:
                tables.extend(self._extract_tables_from_expression(expression.this, alias_map))
        elif isinstance(expression, In):
            # Handle IN expressions (including subqueries)
            if hasattr(expression, 'this') and expression.this:
                tables.extend(self._extract_tables_from_expression(expression.this, alias_map))
            if hasattr(expression, 'expressions'):
                for expr in expression.expressions:
                    tables.extend(self._extract_tables_from_expression(expr, alias_map))
            # Check args for subquery
            if hasattr(expression, 'args') and 'query' in expression.args:
                tables.extend(self._extract_tables_from_expression(expression.args['query'], alias_map))
        elif hasattr(expression, 'this') and expression.this:
            # Handle expressions with 'this' attribute (like From clause)
            tables.extend(self._extract_tables_from_expression(expression.this, alias_map))
        elif hasattr(expression, 'expressions'):
            # Handle expressions with sub-expressions
            for expr in expression.expressions:
                tables.extend(self._extract_tables_from_expression(expr, alias_map))
        
        return tables
    
    def _create_parsed_table_from_table(self, table: Table) -> Optional[ParsedTable]:
        """Create a ParsedTable from a SQLGlot Table object"""
        if not table.this:
            return None
        
        table_name = str(table.this)
        schema = None
        
        # Extract schema from db field if present
        if table.db:
            schema = str(table.db)
        
        # Check if it's a schema.table format (fallback)
        if not schema and '.' in table_name:
            parts = table_name.split('.')
            if len(parts) == 2:
                schema, table_name = parts
        
        # Validate table name
        if not self._is_valid_table_name(table_name):
            return None
        
        return ParsedTable(
            name=table_name,
            schema=schema,
            is_subquery=False
        )
    
    def _is_valid_table_name(self, name: str) -> bool:
        """Check if a table name is valid (not a keyword or alias)"""
        if not name or not name.strip():
            return False
        
        name = name.strip()
        
        # Check for spaces and hyphens in the original name (before converting to uppercase)
        if ' ' in name or '-' in name:
            return False
        
        name = name.upper()
        
        # Skip SQL keywords
        if name in self.sql_keywords:
            return False
        
        # Skip single-letter aliases
        if len(name) == 1 and name in self.common_aliases:
            return False
        
        # Must contain at least one letter and be longer than 1 character
        return len(name) > 1 and any(c.isalpha() for c in name)
    
    def _is_volatile_table(self, parsed: Create) -> bool:
        """Check if CREATE statement creates a volatile table"""
        # Check for VOLATILE keyword in the statement
        sql_str = str(parsed).upper()
        return "VOLATILE" in sql_str
    
    def _is_view(self, parsed: Create) -> bool:
        """Check if CREATE statement creates a view"""
        # Check for VIEW keyword in the statement
        sql_str = str(parsed).upper()
        return "VIEW" in sql_str
    
    # Placeholder methods for extracting specific information
    def _extract_target_table_from_insert(self, parsed: Insert) -> Optional[ParsedTable]:
        """Extract target table from INSERT statement"""
        if hasattr(parsed, 'this') and parsed.this:
            tables = self._extract_tables_from_expression(parsed.this)
            return tables[0] if tables else None
        return None
    
    def _extract_target_table_from_update(self, parsed: Update) -> Optional[ParsedTable]:
        """Extract target table from UPDATE statement"""
        # Build alias map for this UPDATE statement
        alias_map = self._build_alias_map(parsed)
        
        # For Teradata UPDATE A FROM table syntax, the target table is in the FROM clause
        if hasattr(parsed, 'args') and 'from' in parsed.args:
            from_clause = parsed.args['from']
            if from_clause:
                tables = self._extract_tables_from_expression(from_clause, alias_map)
                # Return the first table from the FROM clause as the target
                return tables[0] if tables else None
        
        # Fallback to the standard approach
        if hasattr(parsed, 'this') and parsed.this:
            tables = self._extract_tables_from_expression(parsed.this, alias_map)
            return tables[0] if tables else None
        return None
    
    def _extract_target_table_from_delete(self, parsed: Delete) -> Optional[ParsedTable]:
        """Extract target table from DELETE statement"""
        if hasattr(parsed, 'this') and parsed.this:
            tables = self._extract_tables_from_expression(parsed.this)
            return tables[0] if tables else None
        return None
    
    def _extract_target_table_from_create(self, parsed: Create) -> Optional[ParsedTable]:
        """Extract target table from CREATE statement"""
        if hasattr(parsed, 'this') and parsed.this:
            tables = self._extract_tables_from_expression(parsed.this)
            return tables[0] if tables else None
        return None
    
    def _extract_target_table_from_drop(self, parsed: Drop) -> Optional[ParsedTable]:
        """Extract target table from DROP statement"""
        if hasattr(parsed, 'this') and parsed.this:
            tables = self._extract_tables_from_expression(parsed.this)
            return tables[0] if tables else None
        return None
    
    def _extract_target_table_from_alter(self, parsed: Alter) -> Optional[ParsedTable]:
        """Extract target table from ALTER statement"""
        if hasattr(parsed, 'this') and parsed.this:
            tables = self._extract_tables_from_expression(parsed.this)
            return tables[0] if tables else None
        return None
    
    def _extract_target_table_from_merge(self, parsed: Merge) -> Optional[ParsedTable]:
        """Extract target table from MERGE statement"""
        if hasattr(parsed, 'this') and parsed.this:
            tables = self._extract_tables_from_expression(parsed.this)
            return tables[0] if tables else None
        return None
    
    # Placeholder methods for extracting columns and conditions
    def _extract_columns_from_select(self, parsed: Select) -> List[str]:
        """Extract column names from SELECT statement"""
        # Implementation would extract column names from SELECT clause
        return []
    
    def _extract_columns_from_insert(self, parsed: Insert) -> List[str]:
        """Extract column names from INSERT statement"""
        # Implementation would extract column names from INSERT clause
        return []
    
    def _extract_columns_from_update(self, parsed: Update) -> List[str]:
        """Extract column names from UPDATE statement"""
        # Implementation would extract column names from SET clause
        return []
    
    def _extract_columns_from_create(self, parsed: Create) -> List[str]:
        """Extract column names from CREATE statement"""
        # Implementation would extract column names from CREATE clause
        return []
    
    def _extract_conditions_from_select(self, parsed: Select) -> List[str]:
        """Extract WHERE conditions from SELECT statement"""
        # Implementation would extract WHERE conditions
        return []
    
    def _extract_conditions_from_update(self, parsed: Update) -> List[str]:
        """Extract WHERE conditions from UPDATE statement"""
        # Implementation would extract WHERE conditions
        return []
    
    def _extract_conditions_from_delete(self, parsed: Delete) -> List[str]:
        """Extract WHERE conditions from DELETE statement"""
        # Implementation would extract WHERE conditions
        return []
    
    def _build_alias_map(self, parsed) -> Dict[str, str]:
        """Build alias map for a SQL statement, handling scoping"""
        alias_map = {}
        
        if isinstance(parsed, Update):
            # For UPDATE statements, build alias map from FROM clause
            if hasattr(parsed, 'args') and 'from' in parsed.args:
                from_clause = parsed.args['from']
                if from_clause:
                    self._extract_aliases_from_expression(from_clause, alias_map)
        elif isinstance(parsed, Select):
            # For SELECT statements, build alias map from FROM clause
            if hasattr(parsed, 'args') and 'from' in parsed.args:
                from_clause = parsed.args['from']
                if from_clause:
                    self._extract_aliases_from_expression(from_clause, alias_map)
        
        return alias_map
    
    def _extract_aliases_from_expression(self, expression, alias_map: Dict[str, str]) -> None:
        """Extract aliases from expressions and build alias map"""
        if isinstance(expression, Alias):
            # Handle table aliases
            if isinstance(expression.this, Table):
                table_name = self._get_table_name(expression.this)
                if table_name and expression.alias:
                    alias_map[expression.alias] = table_name
            else:
                # Recursively extract from the aliased expression
                self._extract_aliases_from_expression(expression.this, alias_map)
        elif isinstance(expression, From):
            # Handle FROM clauses - check 'this' attribute first
            if hasattr(expression, 'this') and expression.this:
                self._extract_aliases_from_expression(expression.this, alias_map)
            # Also check expressions attribute
            if hasattr(expression, 'expressions'):
                for expr in expression.expressions:
                    self._extract_aliases_from_expression(expr, alias_map)
        elif isinstance(expression, Table):
            # Handle Table objects directly
            if hasattr(expression, 'alias') and expression.alias:
                table_name = self._get_table_name(expression)
                if table_name:
                    alias_map[expression.alias] = table_name
            # Check for joins in args
            if hasattr(expression, 'args') and 'joins' in expression.args:
                joins = expression.args['joins']
                if joins:
                    for join in joins:
                        self._extract_aliases_from_expression(join, alias_map)
        elif isinstance(expression, Subquery):
            # Handle subqueries - create new scope
            if hasattr(expression, 'this'):
                self._extract_aliases_from_expression(expression.this, alias_map)
            # Also check for alias on the subquery itself
            if hasattr(expression, 'alias') and expression.alias:
                # For subqueries, we need to extract the table name from the SELECT
                if isinstance(expression.this, Select):
                    # Get the table name from the FROM clause of the subquery
                    if hasattr(expression.this, 'args') and 'from' in expression.this.args:
                        from_clause = expression.this.args['from']
                        if from_clause and hasattr(from_clause, 'this'):
                            # Check if from_clause.this is a Table object before calling _get_table_name
                            if isinstance(from_clause.this, Table):
                                table_name = self._get_table_name(from_clause.this)
                                if table_name:
                                    alias_map[expression.alias] = table_name
                            else:
                                # If it's not a Table (e.g., Subquery), recursively extract aliases
                                self._extract_aliases_from_expression(from_clause.this, alias_map)
        elif isinstance(expression, Join):
            # Handle JOIN objects
            if hasattr(expression, 'this'):
                self._extract_aliases_from_expression(expression.this, alias_map)
        elif isinstance(expression, Select):
            # Handle SELECT statements
            if hasattr(expression, 'args') and 'from' in expression.args:
                from_clause = expression.args['from']
                if from_clause:
                    self._extract_aliases_from_expression(from_clause, alias_map)
        elif hasattr(expression, 'expressions'):
            # Handle expressions with sub-expressions
            for expr in expression.expressions:
                self._extract_aliases_from_expression(expr, alias_map)
    
    def _get_table_name(self, table: Table) -> str:
        """Get full table name from Table object"""
        if not table:
            return None
        
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        if table.name:
            parts.append(table.name)
        
        return '.'.join(parts) if parts else None

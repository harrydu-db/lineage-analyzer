#!/usr/bin/env python3
"""
SQL Extractor for Shell Files

This script extracts SQL statements from shell files that contain BTEQ (Basic Teradata Query) blocks.
It can handle various SQL statement types including SELECT, INSERT, UPDATE, DELETE, etc.
"""

import os
import re
import argparse
import logging
from pathlib import Path
from typing import List, Tuple, Optional


class SQLExtractor:
    """Extracts SQL statements from shell files containing BTEQ blocks."""
    
    def __init__(self, input_file: str = None, output_folder: str = None):
        """
        Initialize the SQL extractor.
        
        Args:
            input_file: Path to the input shell file (optional for testing)
            output_folder: Path to the output folder for extracted SQL files (optional for testing)
        """
        self.input_file = Path(input_file) if input_file else None
        self.output_folder = Path(output_folder) if output_folder else None
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def validate_inputs(self) -> bool:
        """
        Validate input file and output folder.
        
        Returns:
            True if inputs are valid, False otherwise
        """
        if self.input_file is None or self.output_folder is None:
            self.logger.error("Input file and output folder must be provided for validation")
            return False
            
        if not self.input_file.exists():
            self.logger.error(f"Input file does not exist: {self.input_file}")
            return False
            
        if not self.input_file.is_file():
            self.logger.error(f"Input path is not a file: {self.input_file}")
            return False
            
        # Create output folder if it doesn't exist
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        return True
        
    def read_file_content(self) -> str:
        """
        Read the content of the input file.
        
        Returns:
            File content as string
        """
        try:
            with open(self.input_file, 'r', encoding='utf-8') as file:
                content = file.read()
            self.logger.info(f"Successfully read file: {self.input_file}")
            return content
        except Exception as e:
            self.logger.error(f"Error reading file {self.input_file}: {e}")
            raise
            
    def extract_bteq_blocks(self, content: str) -> List[Tuple[str, int, int]]:
        """
        Extract BTEQ blocks from the shell script content.
        
        Args:
            content: The shell script content
            
        Returns:
            List of tuples containing (sql_block, start_line, end_line)
        """
        bteq_blocks = []
        
        # Pattern to match BTEQ blocks: bteq <<EOF ... EOF
        # This handles both single-line and multi-line BTEQ blocks
        bteq_pattern = r'bteq\s*<<EOF\s*\n(.*?)\nEOF'
        
        matches = re.finditer(bteq_pattern, content, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            sql_block = match.group(1).strip()
            start_line = content[:match.start()].count('\n') + 1
            end_line = content[:match.end()].count('\n') + 1
            bteq_blocks.append((sql_block, start_line, end_line))
            
        self.logger.info(f"Found {len(bteq_blocks)} BTEQ blocks")
        return bteq_blocks
        
    def remove_comments(self, text: str) -> str:
        """
        Remove SQL comments from the text.
        
        Args:
            text: The text to remove comments from
            
        Returns:
            Text with comments removed
        """
        # Remove block comments /* ... */ - replace with empty string
        text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        
        # Remove line comments --
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Find the position of -- comment
            comment_pos = line.find('--')
            if comment_pos != -1:
                # Keep only the part before the comment
                line = line[:comment_pos].rstrip()
            # Only add non-empty lines
            if line.strip():
                cleaned_lines.append(line)
            
        return '\n'.join(cleaned_lines)
    
    def _is_bteq_command(self, line: str, line_upper: str) -> bool:
        """
        Check if a line is a BTEQ command (case insensitive).
        
        Args:
            line: The original line
            line_upper: The line in uppercase for case-insensitive comparison
            
        Returns:
            True if the line is a BTEQ command, False otherwise
        """
        # BTEQ commands that don't need dot prefix
        bteq_commands_no_dot = [
            'BT', 'ET', 'SLEEP'
        ]
        
        # Check for commands without dot prefix first (with or without semicolon)
        for cmd in bteq_commands_no_dot:
            if (line_upper == cmd or 
                line_upper == cmd + ';' or 
                line_upper.startswith(cmd + ' ') or
                line_upper.startswith(cmd + ';')):
                return True
        
        # BTEQ commands that require dot prefix
        if not line.startswith('.'):
            return False
            
        # Comprehensive list of BTEQ commands with dot prefix (case insensitive)
        bteq_commands_with_dot = [
            '.ABORT', '.ACCOUNT', '.AUTOCONNECT', '.AUTODISCONNECT', '.AUTOLOGON',
            '.BEGQUERY', '.BREAK', '.BT', '.CHECKPOINT', '.CLOSE', '.CONNECT',
            '.CONTINUE', '.DATABASE', '.DEFAULTS', '.DISCARD', '.DISCONNECT',
            '.DISTRIBUTION', '.DUMP', '.ECHO', '.ENDQUERY', '.ERRORCODE', '.ERRORLEVEL',
            '.ERROROUT', '.ET', '.EXIT', '.EXPORT', '.FORMAT', '.GOTO', '.HELP',
            '.IF', '.IMPORT', '.INDICDATA', '.LABEL', '.LAST', '.LOGOFF', '.LOGON',
            '.LOGMECH', '.MACRO', '.MESSAGE', '.NONSTOP', '.NULL', '.PACK', '.PACKET',
            '.PASSWORD', '.PRINT', '.QUERY', '.QUIET', '.QUIT', '.RECORD', '.REPEAT',
            '.REPEATMODE', '.RESET', '.RETRY', '.RETURN', '.RUN', '.RUNFILE', '.SAMPLE',
            '.SESSIONS', '.SET', '.SEVERITY', '.SHOW', '.SID', '.SKIP', '.SLEEP', '.SPOOL',
            '.TDP', '.TERM', '.TIMEOUT', '.TITLE', '.UNPACK', '.WIDTH', '.ZERO'
        ]
        
        # Check if line starts with any BTEQ command with dot prefix
        for cmd in bteq_commands_with_dot:
            if line_upper.startswith(cmd):
                return True
                
        return False
        
    def extract_individual_sql_statements(self, bteq_block: str, start_line: int, end_line: int) -> List[Tuple[str, str, int]]:
        """
        Extract individual SQL statements from a BTEQ block.
        
        Args:
            bteq_block: The BTEQ block content
            start_line: Starting line number in the original file
            end_line: Ending line number in the original file
            
        Returns:
            List of tuples containing (statement_type, sql_statement, line_number)
        """
        sql_statements = []
        
        # First remove comments from the entire block
        cleaned_block = self.remove_comments(bteq_block)
        
        # Remove BTEQ-specific commands and BT;/ET; statements
        lines = cleaned_block.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            line_upper = line.upper()
            
            # Skip BTEQ commands (case insensitive)
            if (self._is_bteq_command(line, line_upper) or
                line == ''):
                continue
            cleaned_lines.append(line)
            
        # Join lines and split by semicolon to get individual statements
        sql_text = '\n'.join(cleaned_lines)
        
        # Split by semicolon and filter out empty statements
        statements = [stmt.strip() for stmt in sql_text.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements):
            if not statement:
                continue
                
            # Determine statement type
            statement_type = self.classify_sql_statement(statement)
            
            # Add semicolon back to the statement
            sql_statement = statement + ';'
            
            # Calculate approximate line number based on statement position
            # This is an approximation since we can't easily map individual statements to exact lines
            # We'll use the start of the BTEQ block plus an offset
            estimated_line = start_line + 1 + i  # +1 for the bteq <<EOF line
            
            sql_statements.append((statement_type, sql_statement, estimated_line))
            
        return sql_statements
        
    def classify_sql_statement(self, statement: str) -> str:
        """
        Classify the type of SQL statement.
        
        Args:
            statement: The SQL statement
            
        Returns:
            Statement type (SELECT, INSERT, UPDATE, DELETE, etc.)
        """
        statement_upper = statement.upper().strip()
        
        if statement_upper.startswith('SELECT'):
            return 'SELECT'
        elif statement_upper.startswith('INSERT'):
            return 'INSERT'
        elif statement_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif statement_upper.startswith('DELETE'):
            return 'DELETE'
        elif statement_upper.startswith('CREATE'):
            return 'CREATE'
        elif statement_upper.startswith('DROP'):
            return 'DROP'
        elif statement_upper.startswith('ALTER'):
            return 'ALTER'
        elif statement_upper.startswith('MERGE'):
            return 'MERGE'
        elif statement_upper.startswith('WITH'):
            return 'CTE'
        else:
            return 'OTHER'
            
    def write_single_sql_file(self, sql_statements: List[Tuple[str, str, int]], 
                             base_filename: str) -> None:
        """
        Write all SQL statements to a single output file.
        
        Args:
            sql_statements: List of (statement_type, sql_statement, line_number)
            base_filename: Base filename for output file
        """
        filename = f"{base_filename}.sql"
        filepath = self.output_folder / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(f"-- Extracted from: {self.input_file.name}\n")
            file.write(f"-- Total statements: {len(sql_statements)}\n")
            file.write("--" + "="*50 + "\n\n")
            
            for i, (stmt_type, sql_stmt, line_num) in enumerate(sql_statements, 1):
                file.write(f"-- Statement {i}: {stmt_type}\n")
                file.write(sql_stmt)
                file.write("\n\n")
                
        self.logger.info(f"Written {len(sql_statements)} statements to {filepath}")
    
    def extract(self) -> bool:
        """
        Main extraction method.
        
        Returns:
            True if extraction was successful, False otherwise
        """
        try:
            if not self.validate_inputs():
                return False
                
            content = self.read_file_content()
            bteq_blocks = self.extract_bteq_blocks(content)
            
            if not bteq_blocks:
                self.logger.warning("No BTEQ blocks found in the file")
                return True
                
            all_sql_statements = []
            
            for bteq_block, start_line, end_line in bteq_blocks:
                sql_statements = self.extract_individual_sql_statements(bteq_block, start_line, end_line)
                all_sql_statements.extend(sql_statements)
                
            if not all_sql_statements:
                self.logger.warning("No SQL statements found in BTEQ blocks")
                return True
                
            # Generate base filename from input file
            base_filename = self.input_file.stem
            
            # Write single output file with all statements
            self.write_single_sql_file(all_sql_statements, base_filename)
            
            self.logger.info(f"Successfully extracted {len(all_sql_statements)} SQL statements")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during extraction: {e}")
            return False


def main():
    """Main function to handle command line arguments and run the extractor."""
    parser = argparse.ArgumentParser(
        description="Extract SQL statements from shell files containing BTEQ blocks"
    )
    parser.add_argument(
        "input_file",
        help="Path to the input shell file"
    )
    parser.add_argument(
        "output_folder",
        help="Path to the output folder for extracted SQL files"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    extractor = SQLExtractor(args.input_file, args.output_folder)
    success = extractor.extract()
    
    if success:
        print(f"SQL extraction completed successfully. Check output folder: {args.output_folder}")
    else:
        print("SQL extraction failed. Check logs for details.")
        exit(1)


if __name__ == "__main__":
    main() 
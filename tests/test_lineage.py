"""
Tests for the ETL Lineage Analyzer
"""

import pytest
import tempfile
import os
from pathlib import Path
from src.lineage import ETLLineageAnalyzer, LineageInfo, TableOperation


class TestETLLineageAnalyzer:
    """Test cases for the ETLLineageAnalyzer class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.analyzer = ETLLineageAnalyzer()

    def test_is_valid_table_name(self):
        """Test table name validation"""
        # Valid table names
        assert self.analyzer.is_valid_table_name("my_table")
        assert self.analyzer.is_valid_table_name("schema.table")
        assert self.analyzer.is_valid_table_name("table_123")
        
        # Invalid table names
        assert not self.analyzer.is_valid_table_name("SELECT")
        assert not self.analyzer.is_valid_table_name("A")  # Single letter alias
        assert not self.analyzer.is_valid_table_name("")
        assert not self.analyzer.is_valid_table_name("table name")  # Contains space

    def test_extract_sql_blocks_from_shell_script(self):
        """Test SQL block extraction from shell scripts"""
        shell_script = """
        #!/bin/bash
        bteq <<EOF
        CREATE VOLATILE TABLE temp_table AS (
            SELECT * FROM source_table
        );
        EOF
        
        bteq <<EOF
        INSERT INTO target_table
        SELECT * FROM temp_table;
        EOF
        """
        
        blocks = self.analyzer.extract_sql_blocks(shell_script)
        assert len(blocks) == 2
        assert "CREATE VOLATILE TABLE" in blocks[0]
        assert "INSERT INTO target_table" in blocks[1]

    def test_extract_sql_blocks_from_sql_file(self):
        """Test SQL block extraction from SQL files"""
        sql_content = """
        -- This is a SQL file
        CREATE VOLATILE TABLE temp_table AS (
            SELECT * FROM source_table
        );
        
        INSERT INTO target_table
        SELECT * FROM temp_table;
        """
        
        blocks = self.analyzer.extract_sql_blocks(sql_content)
        assert len(blocks) == 1
        assert "CREATE VOLATILE TABLE" in blocks[0]
        assert "INSERT INTO target_table" in blocks[0]

    def test_extract_table_names(self):
        """Test table name extraction from SQL"""
        sql = """
        SELECT a.col1, b.col2
        FROM table1 a
        LEFT OUTER JOIN table2 b ON a.id = b.id
        INNER JOIN schema.table3 c ON b.id = c.id
        """
        
        tables = self.analyzer.extract_table_names(sql)
        expected = {"table1", "table2", "schema.table3"}
        assert tables == expected

    def test_analyze_script_with_temp_file(self):
        """Test script analysis with a temporary file"""
        sql_content = """
        CREATE VOLATILE TABLE temp_table AS (
            SELECT * FROM source_table
        );
        
        INSERT INTO target_table
        SELECT * FROM temp_table;
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_file = f.name
        
        try:
            lineage_info = self.analyzer.analyze_script(temp_file)
            
            assert isinstance(lineage_info, LineageInfo)
            assert lineage_info.script_name == os.path.basename(temp_file)
            assert "source_table" in lineage_info.source_tables
            assert "target_table" in lineage_info.target_tables
            assert "temp_table" in lineage_info.volatile_tables
            
        finally:
            os.unlink(temp_file)

    def test_export_to_json(self):
        """Test JSON export functionality"""
        lineage_info = LineageInfo(
            script_name="test.sql",
            volatile_tables=["temp_table"],
            source_tables={"source_table"},
            target_tables={"target_table"},
            operations=[],
            table_relationships={"target_table": ["temp_table"]}
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            self.analyzer.export_to_json(lineage_info, temp_file)
            
            # Verify file was created and contains expected content
            assert os.path.exists(temp_file)
            with open(temp_file, 'r') as f:
                content = f.read()
                assert "test.sql" in content
                assert "source_table" in content
                assert "target_table" in content
                
        finally:
            os.unlink(temp_file)

    def test_export_to_html(self):
        """Test HTML export functionality"""
        lineage_info = LineageInfo(
            script_name="test.sql",
            volatile_tables=["temp_table"],
            source_tables={"source_table"},
            target_tables={"target_table"},
            operations=[],
            table_relationships={"target_table": ["temp_table"]}
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
            temp_file = f.name
        
        try:
            self.analyzer.export_to_html(lineage_info, temp_file)
            
            # Verify file was created and contains expected content
            assert os.path.exists(temp_file)
            with open(temp_file, 'r') as f:
                content = f.read()
                assert "<html>" in content
                assert "test.sql" in content
                assert "source_table" in content
                assert "target_table" in content
                
        finally:
            os.unlink(temp_file)


class TestTableOperation:
    """Test cases for the TableOperation dataclass"""

    def test_table_operation_creation(self):
        """Test TableOperation creation"""
        operation = TableOperation(
            operation_type="CREATE_VOLATILE",
            target_table="temp_table",
            source_tables=["source_table"],
            columns=["col1", "col2"],
            conditions=["WHERE col1 > 0"],
            line_number=10
        )
        
        assert operation.operation_type == "CREATE_VOLATILE"
        assert operation.target_table == "temp_table"
        assert operation.source_tables == ["source_table"]
        assert operation.line_number == 10


class TestLineageInfo:
    """Test cases for the LineageInfo dataclass"""

    def test_lineage_info_creation(self):
        """Test LineageInfo creation"""
        lineage_info = LineageInfo(
            script_name="test.sql",
            volatile_tables=["temp_table"],
            source_tables={"source_table"},
            target_tables={"target_table"},
            operations=[],
            table_relationships={"target_table": ["temp_table"]}
        )
        
        assert lineage_info.script_name == "test.sql"
        assert "temp_table" in lineage_info.volatile_tables
        assert "source_table" in lineage_info.source_tables
        assert "target_table" in lineage_info.target_tables 
"""
Tests for the ETL Lineage Analyzer
"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from src.lineage_analyzer.lineage import ETLLineageAnalyzerSQLGlot, LineageInfo, TableOperation


class TestETLLineageAnalyzer:
    """Test cases for the ETLLineageAnalyzer class"""

    def setup_method(self):
        """Set up test fixtures"""
        self.analyzer = ETLLineageAnalyzerSQLGlot()


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
            # The analyzer correctly identifies temp_table as a source table
            # since it's created from source_table in the CREATE VOLATILE statement
            assert "temp_table" in lineage_info.source_tables
            assert "target_table" in lineage_info.target_tables
            assert "temp_table" in lineage_info.volatile_tables
            
        finally:
            os.unlink(temp_file)

    def test_export_to_json_new_format(self):
        """Test JSON export functionality with new format"""
        # Create a mock lineage info with operations
        operations = [
            TableOperation(
                operation_type="CREATE_VOLATILE",
                target_table="temp_table",
                source_tables=["source_table"],
                columns=[],
                conditions=[],
                line_number=1,
                sql_statement="CREATE VOLATILE TABLE temp_table AS (SELECT * FROM source_table);"
            ),
            TableOperation(
                operation_type="INSERT",
                target_table="target_table",
                source_tables=["temp_table"],
                columns=[],
                conditions=[],
                line_number=5,
                sql_statement="INSERT INTO target_table SELECT * FROM temp_table;"
            )
        ]
        
        lineage_info = LineageInfo(
            script_name="test.sql",
            volatile_tables=["temp_table"],
            source_tables={"source_table", "temp_table"},
            target_tables={"temp_table", "target_table"},
            operations=operations,
            table_relationships={"target_table": ["temp_table"], "temp_table": ["source_table"]},
            warnings=[]
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            self.analyzer.export_to_json(lineage_info, temp_file)
            
            # Verify file was created and contains expected content
            assert os.path.exists(temp_file)
            with open(temp_file, 'r') as f:
                data = json.load(f)
                
                # Check new JSON structure
                assert "script_name" in data
                assert "bteq_statements" in data
                assert "tables" in data
                
                # Check script name
                assert data["script_name"] == "test.sql"
                
                # Check bteq_statements array
                assert isinstance(data["bteq_statements"], list)
                assert len(data["bteq_statements"]) > 0
                
                # Check tables structure
                assert isinstance(data["tables"], dict)
                assert "temp_table" in data["tables"]
                assert "target_table" in data["tables"]
                
                # Check table structure
                temp_table_data = data["tables"]["temp_table"]
                assert "source" in temp_table_data
                assert "target" in temp_table_data
                assert "is_volatile" in temp_table_data
                assert temp_table_data["is_volatile"] == True
                
        finally:
            os.unlink(temp_file)





    def test_extract_operations(self):
        """Test operation extraction from SQL"""
        sql = """
        CREATE VOLATILE TABLE temp_table AS (
            SELECT * FROM source_table
        );
        
        INSERT INTO target_table
        SELECT * FROM temp_table;
        """
        
        operations = self.analyzer.extract_operations(sql)
        
        assert len(operations) == 2
        
        # Check CREATE VOLATILE operation
        create_op = operations[0]
        assert create_op.operation_type == "CREATE_VOLATILE"
        assert create_op.target_table == "temp_table"
        assert "source_table" in create_op.source_tables
        
        # Check INSERT operation
        insert_op = operations[1]
        assert insert_op.operation_type == "INSERT"
        assert insert_op.target_table == "target_table"
        assert "temp_table" in insert_op.source_tables

    def test_process_folder(self):
        """Test folder processing functionality"""
        # Create a temporary directory with test files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test SQL file
            test_sql = os.path.join(temp_dir, "test.sql")
            with open(test_sql, 'w') as f:
                f.write("""
                CREATE VOLATILE TABLE temp_table AS (
                    SELECT * FROM source_table
                );
                
                INSERT INTO target_table
                SELECT * FROM temp_table;
                """)
            
            # Create output directory
            output_dir = os.path.join(temp_dir, "output")
            os.makedirs(output_dir)
            
            # Process the folder
            self.analyzer.process_folder(temp_dir, output_dir)
            
                        # Check that output files were created
            expected_files = [
                "test_sql_lineage.json",
                "test.bteq"
            ]

            for filename in expected_files:
                filepath = os.path.join(output_dir, filename)
                assert os.path.exists(filepath), f"Expected file {filename} was not created"

    def test_create_view_handling(self):
        """Test CREATE VIEW statement handling"""
        sql_content = """
        CREATE VIEW IF NOT EXISTS BIZT.BATCHCHARACTERISTICSDATA_V AS
        SELECT *
        FROM BIZT.BATCHCHARACTERISTICSDATA
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_file = f.name
        
        try:
            lineage_info = self.analyzer.analyze_script(temp_file)
            
            assert isinstance(lineage_info, LineageInfo)
            assert lineage_info.script_name == os.path.basename(temp_file)
            
            # Check that the view is identified as a target table
            assert "BIZT.BATCHCHARACTERISTICSDATA_V" in lineage_info.target_tables
            
            # Check that the source table is identified
            assert "BIZT.BATCHCHARACTERISTICSDATA" in lineage_info.source_tables
            
            # Check that there's one operation
            assert len(lineage_info.operations) == 1
            operation = lineage_info.operations[0]
            assert operation.operation_type == "CREATE_VIEW"
            assert operation.target_table == "BIZT.BATCHCHARACTERISTICSDATA_V"
            assert "BIZT.BATCHCHARACTERISTICSDATA" in operation.source_tables
            
            # Check table relationships
            assert "BIZT.BATCHCHARACTERISTICSDATA_V" in lineage_info.table_relationships
            assert "BIZT.BATCHCHARACTERISTICSDATA" in lineage_info.table_relationships["BIZT.BATCHCHARACTERISTICSDATA_V"]
            
        finally:
            os.unlink(temp_file)

    def test_create_view_variations(self):
        """Test different CREATE VIEW statement variations"""
        test_cases = [
            # Standard CREATE VIEW
            ("CREATE VIEW schema.view_name AS SELECT * FROM table1", "schema.view_name", ["table1"]),
            # CREATE VIEW with IF NOT EXISTS
            ("CREATE VIEW IF NOT EXISTS view_name AS SELECT * FROM table1", "view_name", ["table1"]),
            # CREATE VIEW with schema and IF NOT EXISTS
            ("CREATE VIEW IF NOT EXISTS schema.view_name AS SELECT * FROM schema.table1", "schema.view_name", ["schema.table1"]),
            # CREATE VIEW with multiple source tables
            ("CREATE VIEW view_name AS SELECT * FROM table1 JOIN table2 ON table1.id = table2.id", "view_name", ["table1", "table2"]),
        ]
        
        for sql_content, expected_target, expected_sources in test_cases:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
                f.write(sql_content)
                temp_file = f.name
            
            try:
                lineage_info = self.analyzer.analyze_script(temp_file)
                
                # Check that the view is identified as a target table
                assert expected_target in lineage_info.target_tables
                
                # Check that all source tables are identified
                for source in expected_sources:
                    assert source in lineage_info.source_tables
                
                # Check that there's one operation
                assert len(lineage_info.operations) == 1
                operation = lineage_info.operations[0]
                assert operation.operation_type == "CREATE_VIEW"
                assert operation.target_table == expected_target
                
                # Check that all expected sources are in the operation
                for source in expected_sources:
                    assert source in operation.source_tables
                
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
            line_number=10,
            sql_statement="CREATE VOLATILE TABLE temp_table AS (SELECT * FROM source_table);"
        )
        
        assert operation.operation_type == "CREATE_VOLATILE"
        assert operation.target_table == "temp_table"
        assert operation.source_tables == ["source_table"]
        assert operation.line_number == 10
        assert operation.sql_statement is not None


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
            table_relationships={"target_table": ["temp_table"]},
            warnings=[]
        )
        
        assert lineage_info.script_name == "test.sql"
        assert "temp_table" in lineage_info.volatile_tables
        assert "source_table" in lineage_info.source_tables
        assert "target_table" in lineage_info.target_tables 
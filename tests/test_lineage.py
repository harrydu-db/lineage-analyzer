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
        self.spark_analyzer = ETLLineageAnalyzerSQLGlot(dialect="spark")
        self.spark2_analyzer = ETLLineageAnalyzerSQLGlot(dialect="spark2")


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
            assert "TEMP_TABLE" in lineage_info.source_tables
            assert "TARGET_TABLE" in lineage_info.target_tables
            assert "TEMP_TABLE" in lineage_info.volatile_tables
            
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
        assert create_op.target_table == "TEMP_TABLE"
        assert "SOURCE_TABLE" in create_op.source_tables
        
        # Check INSERT operation
        insert_op = operations[1]
        assert insert_op.operation_type == "INSERT"
        assert insert_op.target_table == "TARGET_TABLE"
        assert "TEMP_TABLE" in insert_op.source_tables

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
            ("CREATE VIEW schema.view_name AS SELECT * FROM table1", "SCHEMA.VIEW_NAME", ["TABLE1"]),
            # CREATE VIEW with IF NOT EXISTS
            ("CREATE VIEW IF NOT EXISTS view_name AS SELECT * FROM table1", "VIEW_NAME", ["TABLE1"]),
            # CREATE VIEW with schema and IF NOT EXISTS
            ("CREATE VIEW IF NOT EXISTS schema.view_name AS SELECT * FROM schema.table1", "SCHEMA.VIEW_NAME", ["SCHEMA.TABLE1"]),
            # CREATE VIEW with multiple source tables
            ("CREATE VIEW view_name AS SELECT * FROM table1 JOIN table2 ON table1.id = table2.id", "VIEW_NAME", ["TABLE1", "TABLE2"]),
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

    def test_case_insensitive_table_names(self):
        """Test that table names are handled case-insensitively"""
        sql_content = """
        CREATE MULTISET VOLATILE TABLE VT_first_fab_enterprise_lot_id AS
        (
            SELECT DISTINCT first_fab_enterprise_lot_id, enterprise_lot_id, TIB_TRACKINGID
            FROM edw.mfg_lot_actv A, bizt.PROMIS_STARTLOT_SOURCELOTLIST_V B
            WHERE a.enterprise_lot_id = SourceLotId
            AND characters(trim(first_fab_enterprise_lot_id)) > 0
            AND TIB_TRACKINGID in (SELECT TIB_TRACKINGID FROM BIZT.BIZT_RESP_MSG_LM_V WHERE LM_PROCESSING_STATUS ='INPROC' AND TRIM(BIZT_NAME) ='LOTSTART' )
            qualify row_number() over(partition by a.enterprise_lot_id order by lot_last_updt_dttm desc)=1
        )WITH DATA
        ON COMMIT PRESERVE ROWS;
        
        INSERT INTO LOTMASTER_BASE_T.MFG_LOT_ACTV (FIRST_FAB_ENTERPRISE_LOT_ID)
        SELECT F.FIRST_FAB_ENTERPRISE_LOT_ID
        FROM VT_FIRST_FAB_ENTERPRISE_LOT_ID F;
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_file = f.name
        
        try:
            lineage_info = self.analyzer.analyze_script(temp_file)
            
            assert isinstance(lineage_info, LineageInfo)
            assert lineage_info.script_name == os.path.basename(temp_file)
            
            # Check that table names are normalized to uppercase
            assert "VT_FIRST_FAB_ENTERPRISE_LOT_ID" in lineage_info.target_tables
            assert "VT_FIRST_FAB_ENTERPRISE_LOT_ID" in lineage_info.volatile_tables
            assert "EDW.MFG_LOT_ACTV" in lineage_info.source_tables
            assert "BIZT.PROMIS_STARTLOT_SOURCELOTLIST_V" in lineage_info.source_tables
            assert "LOTMASTER_BASE_T.MFG_LOT_ACTV" in lineage_info.target_tables
            
            # Verify there are no duplicate table entries with different cases
            target_tables_lower = {table.lower() for table in lineage_info.target_tables}
            source_tables_lower = {table.lower() for table in lineage_info.source_tables}
            volatile_tables_lower = {table.lower() for table in lineage_info.volatile_tables}
            
            # Check that we don't have duplicates (same table with different cases)
            assert len(target_tables_lower) == len(lineage_info.target_tables), "Duplicate table names found in target_tables"
            assert len(source_tables_lower) == len(lineage_info.source_tables), "Duplicate table names found in source_tables"
            assert len(volatile_tables_lower) == len(lineage_info.volatile_tables), "Duplicate table names found in volatile_tables"
            
            # Check that the volatile table is correctly identified
            assert "VT_FIRST_FAB_ENTERPRISE_LOT_ID" in lineage_info.volatile_tables
            
            # Check operations
            assert len(lineage_info.operations) == 2
            
            # Check CREATE VOLATILE operation
            create_op = lineage_info.operations[0]
            assert create_op.operation_type == "CREATE_VOLATILE"
            assert create_op.target_table == "VT_FIRST_FAB_ENTERPRISE_LOT_ID"
            assert create_op.is_volatile == True
            
            # Check INSERT operation
            insert_op = lineage_info.operations[1]
            assert insert_op.operation_type == "INSERT"
            assert insert_op.target_table == "LOTMASTER_BASE_T.MFG_LOT_ACTV"
            assert "VT_FIRST_FAB_ENTERPRISE_LOT_ID" in insert_op.source_tables
            
        finally:
            os.unlink(temp_file)

    def test_table_name_normalization_with_schemas(self):
        """Test that table names with schemas are normalized correctly"""
        sql_content = """
        CREATE VOLATILE TABLE temp_table AS (
            SELECT * FROM schema1.source_table
        );
        
        INSERT INTO Schema2.Target_Table
        SELECT * FROM temp_table;
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_file = f.name
        
        try:
            lineage_info = self.analyzer.analyze_script(temp_file)
            
            # Check that schema names are also normalized to uppercase
            assert "SCHEMA1.SOURCE_TABLE" in lineage_info.source_tables
            assert "SCHEMA2.TARGET_TABLE" in lineage_info.target_tables
            assert "TEMP_TABLE" in lineage_info.target_tables
            assert "TEMP_TABLE" in lineage_info.volatile_tables
            
            # Verify no duplicates exist
            all_tables = list(lineage_info.source_tables) + list(lineage_info.target_tables)
            all_tables_lower = {table.lower() for table in all_tables}
            assert len(all_tables_lower) == len(set(all_tables)), "Duplicate table names found"
            
        finally:
            os.unlink(temp_file)

    def test_spark_dialect_support(self):
        """Test that Spark dialect is properly supported"""
        # Test Spark SQL syntax
        spark_sql = """
        CREATE TABLE IF NOT EXISTS spark_table AS
        SELECT 
            col1,
            col2,
            col3
        FROM source_table
        WHERE col1 > 0
        """
        
        # Test with Spark dialect using temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(spark_sql)
            temp_file = f.name
        
        try:
            lineage_info = self.spark_analyzer.analyze_script(temp_file)
            
            assert lineage_info is not None
            assert len(lineage_info.operations) > 0
            
            # Check that the operation was parsed correctly
            create_operation = next((op for op in lineage_info.operations if op.operation_type == "CREATE"), None)
            assert create_operation is not None
            assert create_operation.target_table == "SPARK_TABLE"
            assert "SOURCE_TABLE" in create_operation.source_tables
        finally:
            os.unlink(temp_file)

    def test_spark2_dialect_support(self):
        """Test that Spark2 dialect is properly supported"""
        # Test Spark2 SQL syntax
        spark2_sql = """
        CREATE OR REPLACE TABLE spark2_table AS
        SELECT 
            col1,
            col2,
            col3
        FROM source_table
        WHERE col1 > 0
        """
        
        # Test with Spark2 dialect using temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(spark2_sql)
            temp_file = f.name
        
        try:
            lineage_info = self.spark2_analyzer.analyze_script(temp_file)
            
            assert lineage_info is not None
            assert len(lineage_info.operations) > 0
            
            # Check that the operation was parsed correctly
            create_operation = next((op for op in lineage_info.operations if op.operation_type == "CREATE"), None)
            assert create_operation is not None
            assert create_operation.target_table == "SPARK2_TABLE"
            assert "SOURCE_TABLE" in create_operation.source_tables
        finally:
            os.unlink(temp_file)

    def test_invalid_dialect_raises_error(self):
        """Test that invalid dialect raises ValueError"""
        with pytest.raises(ValueError, match="Unsupported dialect"):
            ETLLineageAnalyzerSQLGlot(dialect="invalid_dialect")


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
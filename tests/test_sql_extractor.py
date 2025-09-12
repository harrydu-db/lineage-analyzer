#!/usr/bin/env python3
"""
Unit tests for sql_extractor.py module
"""

import unittest
import tempfile
import shutil
from pathlib import Path
import sys
import os

# Add the src directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_extractor import SQLExtractor


class TestSQLExtractor(unittest.TestCase):
    """Test cases for SQLExtractor class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        self.output_folder = self.temp_path / "output"
        self.output_folder.mkdir()
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir)
    
    def test_extract_success_with_bteq_script(self):
        """Test successful extraction from BTEQ script"""
        # Create a test shell script
        test_script = self.temp_path / "test_script.sh"
        test_script.write_text("""
#!/bin/bash
echo "Starting script"

bteq <<EOF
.logon edwpc.nxp.com/user,password;
.SET ECHOREQ OFF;

CREATE VOLATILE TABLE test_table AS
(SELECT * FROM source_table)
WITH DATA ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN GOTO ErrorHandle
BT;
EOF

echo "Script completed"
""")
        
        extractor = SQLExtractor(str(test_script), str(self.output_folder))
        success = extractor.extract()
        
        self.assertTrue(success)
        
        # Check that output file was created
        output_file = self.output_folder / "test_script.sql"
        self.assertTrue(output_file.exists())
        
        # Check content
        content = output_file.read_text()
        self.assertIn("CREATE VOLATILE TABLE test_table", content)
        self.assertIn("SELECT * FROM source_table", content)
        # BTEQ control statements should be removed
        self.assertNotIn(".logon", content)
        self.assertNotIn(".SET", content)
        self.assertNotIn(".IF", content)
        self.assertNotIn("BT;", content)
    
    def test_extract_success_with_multiple_statements(self):
        """Test extraction with multiple SQL statements"""
        test_script = self.temp_path / "multi_script.sh"
        test_script.write_text("""
bteq <<EOF
.logon user,pass;
CREATE TABLE table1 (id INT);
INSERT INTO table1 VALUES (1);
SELECT * FROM table1;
EOF
""")
        
        extractor = SQLExtractor(str(test_script), str(self.output_folder))
        success = extractor.extract()
        
        self.assertTrue(success)
        
        output_file = self.output_folder / "multi_script.sql"
        self.assertTrue(output_file.exists())
        
        content = output_file.read_text()
        self.assertIn("CREATE TABLE table1", content)
        self.assertIn("INSERT INTO table1", content)
        self.assertIn("SELECT * FROM table1", content)
    
    def test_extract_no_bteq_blocks(self):
        """Test extraction from file with no BTEQ blocks"""
        test_script = self.temp_path / "no_sql.sh"
        test_script.write_text("#!/bin/bash\necho 'No SQL here'")
        
        extractor = SQLExtractor(str(test_script), str(self.output_folder))
        success = extractor.extract()
        
        # Should succeed but with warning
        self.assertTrue(success)
        
        # No output file should be created
        output_file = self.output_folder / "no_sql.sql"
        self.assertFalse(output_file.exists())
    
    def test_extract_nonexistent_file(self):
        """Test extraction from non-existent file"""
        nonexistent_file = self.temp_path / "nonexistent.sh"
        
        extractor = SQLExtractor(str(nonexistent_file), str(self.output_folder))
        success = extractor.extract()
        
        self.assertFalse(success)
    
    def test_extract_with_complex_bteq_commands(self):
        """Test extraction with various BTEQ control commands"""
        test_script = self.temp_path / "complex_script.sh"
        test_script.write_text("""
bteq <<EOF
.logon user,pass;
.LOGOFF;
.SET ECHOREQ OFF;
.SET ERROROUT STDOUT;
.IF ERRORCODE <> 0 THEN GOTO ErrorHandle
.LABEL ErrorHandle
.EXPORT FILE=test.txt
.IMPORT FILE=test.txt
.QUIT;
.BT;
.ET;
.GOTO ErrorHandle
.SEVERITY 8
.ERRORLEVEL 1
.ECHOREQ ON
.ERROROUT STDERR
.TITLEDASHES OFF
.WIDTH 200
.RETRY 3
.RUN FILE=test.sql
.REPEAT 5
.SHOW DATABASE;
CREATE TABLE test (id INT);
EOF
""")
        
        extractor = SQLExtractor(str(test_script), str(self.output_folder))
        success = extractor.extract()
        
        self.assertTrue(success)
        
        output_file = self.output_folder / "complex_script.sql"
        self.assertTrue(output_file.exists())
        
        content = output_file.read_text()
        # SQL should be preserved
        self.assertIn("CREATE TABLE test", content)
        
        # All BTEQ control statements should be removed
        control_statements = [
            ".logon", ".LOGOFF", ".SET", ".IF", ".LABEL", ".EXPORT", 
            ".IMPORT", ".QUIT", ".BT", ".ET", ".GOTO", ".SEVERITY", 
            ".ERRORLEVEL", ".ECHOREQ", ".ERROROUT", ".TITLEDASHES", 
            ".WIDTH", ".RETRY", ".RUN", ".REPEAT", ".SHOW"
        ]
        
        for stmt in control_statements:
            self.assertNotIn(stmt.lower(), content.lower())
    
    def test_extract_with_comments(self):
        """Test extraction with SQL comments"""
        test_script = self.temp_path / "commented_script.sh"
        test_script.write_text("""
bteq <<EOF
.logon user,pass;
-- This is a comment
CREATE TABLE test (id INT);
/* Multi-line
   comment */
INSERT INTO test VALUES (1);
EOF
""")
        
        extractor = SQLExtractor(str(test_script), str(self.output_folder))
        success = extractor.extract()
        
        self.assertTrue(success)
        
        output_file = self.output_folder / "commented_script.sql"
        self.assertTrue(output_file.exists())
        
        content = output_file.read_text()
        self.assertIn("CREATE TABLE test", content)
        self.assertIn("INSERT INTO test", content)
        # Comments should be removed
        self.assertNotIn("-- This is a comment", content)
        self.assertNotIn("/* Multi-line", content)


class TestSQLExtractorIntegration(unittest.TestCase):
    """Integration tests for SQLExtractor"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        self.output_folder = self.temp_path / "output"
        self.output_folder.mkdir()
    
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir)
    
    def test_end_to_end_extraction(self):
        """Test complete end-to-end extraction process"""
        # Create a realistic shell script
        script_content = """
#!/bin/bash
## Set DS profile
. /home/dwops/ebi/lotmaster/profile/set_ebi_ds_env

bteq <<EOF
.logon edwpc.nxp.com/edwp_lotmaster_etl,xyz123;
.SET ECHOREQ OFF;
.SET ERROROUT STDOUT;

CREATE MULTISET VOLATILE TABLE BIZT_CONSUME_MATERIALS_N AS
( 
SELECT DISTINCT 
B.LOT_KEY AS SRC_LOT_KEY, 
COALESCE(C.DIFFUSION_BATCH_ID,LAST_FAB_LOT_ID) AS DIFFUSION_BATCH
FROM BIZT.BIZT_CONSUME_MATERIALS_WT_V A
LEFT OUTER JOIN LOTMASTER.LOT_ID_ASGNMT B
ON B.ENTERPRISE_LOT_ID = A.LOT_ID
)WITH DATA ON COMMIT PRESERVE ROWS;

    .IF ERRORCODE <> 0 THEN GOTO SevereErrorHandle
    .BT;
    
    INSERT INTO BIZT.BIZT_CONSUME_MATERIALS_N
    SELECT * FROM BIZT_CONSUME_MATERIALS_N;
    
    .IF ERRORCODE <> 0 THEN GOTO SevereErrorHandle
    .BT;
EOF

echo "Script completed"
"""
        
        # Create test script file
        script_file = self.temp_path / "test_etl.sh"
        script_file.write_text(script_content)
        
        # Extract SQL
        extractor = SQLExtractor(str(script_file), str(self.output_folder))
        success = extractor.extract()
        
        # Verify results
        self.assertTrue(success)
        
        output_file = self.output_folder / "test_etl.sql"
        self.assertTrue(output_file.exists())
        
        sql_content = output_file.read_text()
        
        # Should contain SQL statements
        self.assertIn("CREATE MULTISET VOLATILE TABLE BIZT_CONSUME_MATERIALS_N", sql_content)
        self.assertIn("INSERT INTO BIZT.BIZT_CONSUME_MATERIALS_N", sql_content)
        
        # Should not contain BTEQ control statements
        self.assertNotIn(".logon", sql_content)
        self.assertNotIn(".SET", sql_content)
        self.assertNotIn(".IF", sql_content)
        self.assertNotIn("BT;", sql_content)


if __name__ == '__main__':
    unittest.main()
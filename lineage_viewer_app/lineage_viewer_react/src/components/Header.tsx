import React, { useRef } from 'react';
import { LineageData } from '../types/LineageData';

// Function to convert scripts data format to our app format (same as App.tsx)
const convertScriptsData = (scripts: { [key: string]: any }): LineageData => {
  console.log('Converting scripts data:', scripts);
  const tables: { [key: string]: any } = {};
  const statements: { [key: string]: any } = {};
  
  // Convert tables from all scripts
  Object.values(scripts).forEach((scriptData: any) => {
    const scriptTables = scriptData.tables || {};
    Object.entries(scriptTables).forEach(([tableName, tableData]: [string, any]) => {
      // Convert table name to uppercase
      const upperTableName = tableName.toUpperCase();
      console.log(`Converting table ${upperTableName} from script ${scriptData.script_name}:`, tableData);
      
      // Convert source relationships (tables this table reads from)
      const sources = (tableData.source || []).map((source: any) => {
        const sourceStatements = (source.operation || []).map((op: number) => {
          const stmtId = `${scriptData.script_name}:${op}`;
          return {
            id: stmtId,
            sql: (scriptData.bteq_statements || [])[op] || 'No SQL available',
            file: scriptData.script_name || 'unknown',
            line: op + 1
          };
        });
        
        return {
          table: source.name.toUpperCase(),
          operations: (source.operation || []).map((op: number) => `${scriptData.script_name}:${op}`),
          statements: sourceStatements
        };
      });
      
      // Convert target relationships (tables this table writes to)
      const targets = (tableData.target || []).map((target: any) => {
        const targetStatements = (target.operation || []).map((op: number) => {
          const stmtId = `${scriptData.script_name}:${op}`;
          return {
            id: stmtId,
            sql: (scriptData.bteq_statements || [])[op] || 'No SQL available',
            file: scriptData.script_name || 'unknown',
            line: op + 1
          };
        });
        
        return {
          table: target.name.toUpperCase(),
          operations: (target.operation || []).map((op: number) => `${scriptData.script_name}:${op}`),
          statements: targetStatements
        };
      });
      
      // Store table with script context
      const fullTableName = `${scriptData.script_name}::${upperTableName}`;
      tables[fullTableName] = {
        name: upperTableName,
        owner: 'unknown',
        isVolatile: tableData.is_volatile || false,
        isView: tableData.is_view || false,
        sources: sources,
        targets: targets,
        script: scriptData.script_name
      };
      console.log(`Created table ${fullTableName}:`, tables[fullTableName]);
    });
  });
  
  // Convert statements from all scripts
  let statementIdCounter = 0;
  Object.values(scripts).forEach((scriptData: any) => {
    (scriptData.bteq_statements || []).forEach((sql: string, index: number) => {
      const stmtId = `stmt_${statementIdCounter++}`;
      statements[stmtId] = {
        id: stmtId,
        sql: sql,
        file: scriptData.script_name || 'unknown',
        line: index + 1
      };
    });
  });
  console.log('Created statements:', Object.keys(statements).length);
  
  // Calculate summary to match old implementation
  let totalTables = 0;
  let sourceTables = 0;
  let targetTables = 0;
  let volatileTables = 0;
  let totalOperations = 0;
  
  // Count tables across all scripts (like old implementation)
  Object.values(scripts).forEach((scriptData: any) => {
    const scriptTables = scriptData.tables || {};
    totalTables += Object.keys(scriptTables).length;
    sourceTables += Object.values(scriptTables).filter((t: any) => t.source && t.source.length > 0).length;
    targetTables += Object.values(scriptTables).filter((t: any) => t.target && t.target.length > 0).length;
    volatileTables += Object.values(scriptTables).filter((t: any) => t.is_volatile).length;
  });
  
  // Sum up all operations from all scripts
  Object.values(scripts).forEach((scriptData: any) => {
    totalOperations += (scriptData.bteq_statements || []).length;
  });
  
  const scriptCount = Object.keys(scripts).length;
  const scriptDisplay = '';
  
  console.log('=== SUMMARY DEBUG ===');
  console.log('Total tables (across all scripts):', totalTables);
  console.log('Source tables (have source relationships):', sourceTables);
  console.log('Target tables (have target relationships):', targetTables);
  console.log('Volatile tables:', volatileTables);
  console.log('Total operations:', totalOperations);
  console.log('Script count:', scriptCount);
  console.log('=== END DEBUG ===');
  
  // Post-process to merge table properties across scripts for non-volatile tables only
  // This ensures that if any script defines a non-volatile table as a view, it's treated as a view globally
  const mergedTableProperties: { [key: string]: { isView: boolean } } = {};
  
  Object.values(scripts).forEach((scriptData: any) => {
    Object.entries(scriptData.tables || {}).forEach(([tableName, tableData]: [string, any]) => {
      // Only merge properties for non-volatile tables (volatile tables are script-specific)
      if (!tableData.is_volatile) {
        if (!mergedTableProperties[tableName]) {
          mergedTableProperties[tableName] = {
            isView: false
          };
        }
        // If any definition has is_view: true, treat the table as a view
        if (tableData.is_view) {
          mergedTableProperties[tableName].isView = true;
        }
      }
    });
  });
  
  // Update all table entries with merged properties (only for non-volatile tables)
  Object.keys(tables).forEach(tableKey => {
    const tableName = tables[tableKey].name;
    const tableData = tables[tableKey];
    // Only apply merging to non-volatile tables
    if (!tableData.isVolatile && mergedTableProperties[tableName]) {
      tables[tableKey].isView = mergedTableProperties[tableName].isView;
    }
  });
  
  return {
    tables,
    statements,
    scripts, // Include scripts data for SQL statements
    summary: {
      totalTables,
      sourceTables,
      targetTables,
      volatileTables,
      totalOperations,
      scriptCount,
      scriptDisplay
    }
  };
};

interface HeaderProps {
  onDataLoad: (data: LineageData) => void;
}

const Header: React.FC<HeaderProps> = ({ onDataLoad }) => {
  const fileRef = useRef<HTMLInputElement>(null);
  const folderRef = useRef<HTMLInputElement>(null);

  const loadFile = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const rawData = JSON.parse(e.target?.result as string);
        console.log('Loaded raw JSON data:', rawData);
        
        // Convert single file data to scripts format (same as folder loading)
        const scriptName = rawData.script_name || file.name.replace('_sql_lineage.json', '');
        const scripts = {
          [scriptName]: {
            script_name: scriptName,
            tables: rawData.tables || {},
            bteq_statements: rawData.bteq_statements || []
          }
        };
        
        // Use the same conversion function as folder loading
        const data = convertScriptsData(scripts);
        console.log('Converted JSON data:', data);
        console.log('Data summary:', data.summary);
        onDataLoad(data);
      } catch (error) {
        console.error('Error parsing JSON file:', error);
        alert('Error parsing JSON file. Please check the file format.');
      }
    };
    reader.readAsText(file);
  };

  const handleLoadFileClick = () => {
    fileRef.current?.click();
  };

  const loadFolder = (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    console.log('Folder selection - files:', files);
    
    if (!files || files.length === 0) {
      console.log('No files selected');
      return;
    }

    console.log(`Found ${files.length} files in folder`);
    
    // Find all JSON files in the folder
    const jsonFiles = Array.from(files).filter(file => file.name.endsWith('.json'));
    console.log(`Found ${jsonFiles.length} JSON files:`, jsonFiles.map(f => f.name));
    
    if (jsonFiles.length === 0) {
      console.log('No JSON files found in folder');
      alert('No JSON files found in the selected folder.');
      return;
    }
    
    // Process all JSON files and create scripts structure (same as App.tsx)
    const scripts: { [key: string]: any } = {};
    let processedFiles = 0;
    
    const processFile = (file: File, index: number) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const reportData = JSON.parse(e.target?.result as string);
          console.log(`Processing file ${index + 1}/${jsonFiles.length}: ${file.name}`, reportData);
          
          // Create script entry using script_name from the data
          const scriptName = reportData.script_name || file.name.replace('_sql_lineage.json', '');
          scripts[scriptName] = {
            script_name: scriptName,
            tables: reportData.tables || {},
            bteq_statements: reportData.bteq_statements || []
          };
          
          processedFiles++;
          
          // If all files processed, convert using the same function as App.tsx
          if (processedFiles === jsonFiles.length) {
            const convertedData = convertScriptsData(scripts);
            
            // Update script count to reflect actual number of files
            convertedData.summary.scriptCount = jsonFiles.length;
            
            console.log('Final converted data:', convertedData);
            onDataLoad(convertedData);
          }
        } catch (error) {
          console.error(`Error parsing JSON file ${file.name}:`, error);
          alert(`Error parsing JSON file ${file.name}. Please check the file format.`);
        }
      };
      reader.readAsText(file);
    };
    
    // Process all JSON files
    jsonFiles.forEach((file, index) => {
      processFile(file, index);
    });
    
    // Reset the input so the same folder can be selected again
    event.target.value = '';
  };


  return (
    <div className="header">
      <div className="header-content">
        <div className="header-title">
          <h1>🔗 ETL Lineage Viewer</h1>
          <p>Visualize data lineage relationships from your ETL scripts</p>
          <div className="header-actions">
            <input 
              type="file" 
              ref={fileRef}
              accept=".json" 
              onChange={loadFile}
              style={{ display: 'none' }}
            />
            <button 
              onClick={handleLoadFileClick}
              className="header-btn header-btn-file"
              title="Load a single JSON lineage file to visualize data relationships"
            >
              📄 Load Lineage File
            </button>
            <input 
              type="file" 
              ref={folderRef}
              {...({ webkitdirectory: '' } as any)}
              multiple 
              onChange={loadFolder}
              style={{ display: 'none' }}
            />
            <button 
              onClick={() => folderRef.current?.click()}
              className="header-btn header-btn-folder"
              title="Load multiple JSON lineage files from a folder to analyze complete data lineage"
            >
              📁 Load Folder
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Header;

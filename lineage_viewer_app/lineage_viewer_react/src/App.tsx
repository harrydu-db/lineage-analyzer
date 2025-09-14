import React, { useState, useEffect } from 'react';
import './App.css';
import Header from './components/Header';
import TabSection from './components/TabSection';
import { LineageData } from './types/LineageData';

// Function to convert scripts data format to our app format (like old implementation)
const convertScriptsData = (scripts: { [key: string]: any }): LineageData => {
  const tables: { [key: string]: any } = {};
  const statements: { [key: string]: any } = {};
  
  // Convert tables from all scripts
  Object.values(scripts).forEach((scriptData: any) => {
    const scriptTables = scriptData.tables || {};
    Object.entries(scriptTables).forEach(([tableName, tableData]: [string, any]) => {
      // Convert table name to uppercase
      const upperTableName = tableName.toUpperCase();
      
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
        sources,
        targets,
        script: scriptData.script_name
      };
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


function App() {
  const [lineageData, setLineageData] = useState<LineageData | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [userLoadedData, setUserLoadedData] = useState(false);

  const handleDataLoad = (data: LineageData) => {
    // Mark that user has manually loaded data
    setUserLoadedData(true);
    
    // Clear existing data and set new data
    setLineageData(null); // Clear first
    setSelectedTable(null);
    
    // Set new data
    setLineageData(data);
  };

  // Load folder data on component mount (only if user hasn't manually loaded data)
  useEffect(() => {
    if (userLoadedData) {
      return;
    }
    
    const loadFolderData = async () => {
      try {
        setIsLoading(true);
        // Load the all_lineage.txt file to get list of JSON files
        const response = await fetch('./report/all_lineage.txt');
        if (response.ok) {
          const text = await response.text();
          const jsonFiles = text.trim().split('\n').filter(line => line.endsWith('.json'));
          
          // Load all JSON files and create scripts structure like old implementation
          const scripts: { [key: string]: any } = {};
          
          for (const fileName of jsonFiles) {
            try {
              const fileResponse = await fetch(`./report/${fileName}`);
              if (fileResponse.ok) {
                const reportData = await fileResponse.json();
                
                // Create script entry using script_name from the data
                const scriptName = reportData.script_name || fileName.replace('_sql_lineage.json', '');
                scripts[scriptName] = {
                  script_name: scriptName,
                  tables: reportData.tables || {},
                  bteq_statements: reportData.bteq_statements || []
                };
              }
            } catch (fileError) {
              console.error(`Error loading ${fileName}:`, fileError);
            }
          }
          
          // Convert scripts data to our format
          const convertedData = convertScriptsData(scripts);
          
          // Update script count to reflect actual number of files
          convertedData.summary.scriptCount = jsonFiles.length;
          
          setLineageData(convertedData);
        } else {
          console.error('Could not load all_lineage.txt:', response.statusText);
        }
      } catch (error) {
        console.error('Error loading folder data:', error);
      } finally {
        setIsLoading(false);
      }
    };

    loadFolderData();
  }, [userLoadedData]);

  return (
    <div className="container">
      <Header onDataLoad={handleDataLoad} />
      {isLoading ? (
        <div className="loading" style={{ padding: '40px', textAlign: 'center' }}>
          <h3>🔄 Loading data from report folder...</h3>
          <p>Please wait while we load the lineage data</p>
        </div>
      ) : lineageData ? (
        <TabSection 
          data={lineageData} 
          selectedTable={selectedTable}
          onTableSelect={setSelectedTable}
        />
      ) : (
        <div className="loading" style={{ padding: '40px', textAlign: 'center' }}>
          <h3>❌ Failed to load data</h3>
          <p>Could not load data from report folder. Please try loading a file manually.</p>
        </div>
      )}
    </div>
  );
}

export default App;
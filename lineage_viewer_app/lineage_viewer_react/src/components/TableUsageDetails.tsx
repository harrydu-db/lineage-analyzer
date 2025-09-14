import React, { useState, useMemo, useCallback, memo } from 'react';
import { Table, Statement, LineageData } from '../types/LineageData';

interface TableUsageDetailsProps {
  table: Table;
  data: LineageData;
}

interface StatementWithScript {
  statement: Statement;
  script: string;
}

interface ScriptGroup {
  scriptName: string;
  statements: StatementWithScript[];
}

interface ExpandedScripts {
  [scriptName: string]: boolean;
}

const TableUsageDetails: React.FC<TableUsageDetailsProps> = ({ table, data }) => {
  const [activeTab, setActiveTab] = useState<'source' | 'target'>('source');
  const [expandedScripts, setExpandedScripts] = useState<ExpandedScripts>({});
  const [maxStatementsPerGroup, setMaxStatementsPerGroup] = useState(50);

  // Toggle script expansion with useCallback
  const toggleScript = useCallback((scriptName: string) => {
    setExpandedScripts(prev => ({
      ...prev,
      [scriptName]: !prev[scriptName]
    }));
  }, []);

  // Helper function to deduplicate statements by script name and statement index
  const deduplicateStatements = (statements: StatementWithScript[]): StatementWithScript[] => {
    const seen = new Set<string>();
    return statements.filter(stmtWithScript => {
      // Use just the statement ID since it already contains script name and index
      const key = stmtWithScript.statement.id;
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
  };

  // Find all statements that use this table as a source
  const sourceUsage = useMemo(() => {
    const allStatements: StatementWithScript[] = [];
    
    // Look through all tables to find where this table is used as a source
    Object.entries(data.tables).forEach(([tableKey, tableData]) => {
      if (tableData.sources) {
        tableData.sources.forEach(source => {
          if (source.table === table.name) {
            // This table is used as a source by tableData
            const scriptName = tableData.script || 'Unknown';
            
            // Add statements from this source relationship
            source.statements.forEach(stmt => {
              allStatements.push({
                statement: stmt,
                script: scriptName
              });
            });
          }
        });
      }
    });
    
    // Deduplicate all statements first
    const uniqueStatements = deduplicateStatements(allStatements);
    
    // Group by script name
    const scriptGroups: { [scriptName: string]: StatementWithScript[] } = {};
    uniqueStatements.forEach(stmtWithScript => {
      if (!scriptGroups[stmtWithScript.script]) {
        scriptGroups[stmtWithScript.script] = [];
      }
      scriptGroups[stmtWithScript.script].push(stmtWithScript);
    });
    
    const result = Object.entries(scriptGroups).map(([scriptName, statements]) => ({
      scriptName,
      statements: statements.sort((a, b) => a.statement.line - b.statement.line)
    }));
    
    return result;
  }, [table.name, data.tables, table.script]);

  // Find all statements that use this table as a target
  const targetUsage = useMemo(() => {
    const allStatements: StatementWithScript[] = [];
    
    // Look through all tables to find where this table is used as a target
    Object.entries(data.tables).forEach(([tableKey, tableData]) => {
      if (tableData.targets) {
        tableData.targets.forEach(target => {
          if (target.table === table.name) {
            // This table is used as a target by tableData
            const scriptName = tableData.script || 'Unknown';
            
            // Add statements from this target relationship
            target.statements.forEach(stmt => {
              allStatements.push({
                statement: stmt,
                script: scriptName
              });
            });
          }
        });
      }
    });
    
    // Deduplicate all statements first
    const uniqueStatements = deduplicateStatements(allStatements);
    
    // Group by script name
    const scriptGroups: { [scriptName: string]: StatementWithScript[] } = {};
    uniqueStatements.forEach(stmtWithScript => {
      if (!scriptGroups[stmtWithScript.script]) {
        scriptGroups[stmtWithScript.script] = [];
      }
      scriptGroups[stmtWithScript.script].push(stmtWithScript);
    });
    
    const result = Object.entries(scriptGroups).map(([scriptName, statements]) => ({
      scriptName,
      statements: statements.sort((a, b) => a.statement.line - b.statement.line)
    }));
    
    return result;
  }, [table.name, data.tables, table.script]);

  const renderScriptGroup = useCallback((group: ScriptGroup) => {
    const isExpanded = expandedScripts[group.scriptName] ?? false; // Default to collapsed
    
    return (
      <div key={group.scriptName} className="script-group">
        <div 
          className="script-header"
          onClick={() => toggleScript(group.scriptName)}
        >
          <span className={`toggle-icon ${isExpanded ? 'expanded' : ''}`}>▶</span>
          <span className="script-name">📄 {group.scriptName} ({group.statements.length})</span>
        </div>
        {isExpanded && (
          <div className="statements-list expanded">
            {(() => {
              const hasMoreStatements = group.statements.length > maxStatementsPerGroup;
              const statementsToShow = hasMoreStatements ? group.statements.slice(0, maxStatementsPerGroup) : group.statements;
              
              return (
                <>
                  {statementsToShow.map((stmtWithScript, index) => (
                    <div
                      key={`${stmtWithScript.statement.id}-${index}`}
                      className="statement-item"
                    >
                      <div className="statement-header">
                        <span className="statement-number">Statement {stmtWithScript.statement.line}</span>
                      </div>
                      <div className="statement-content">
                        <pre>{stmtWithScript.statement.sql}</pre>
                      </div>
                    </div>
                  ))}
                  {hasMoreStatements && (
                    <div className="load-more-statements" style={{ 
                      padding: '10px', 
                      textAlign: 'center', 
                      color: '#007bff', 
                      cursor: 'pointer',
                      borderTop: '1px solid #eee'
                    }}
                    onClick={() => setMaxStatementsPerGroup(prev => prev + 50)}
                    >
                      Load more statements ({group.statements.length - maxStatementsPerGroup} remaining)
                    </div>
                  )}
                </>
              );
            })()}
          </div>
        )}
      </div>
    );
  }, [expandedScripts, toggleScript, maxStatementsPerGroup]);

  return (
    <div className="table-usage-details">
      <div className="table-header">
        <div className="table-name">
          {table.name}
          {table.isVolatile && <span className="volatile-badge">Volatile</span>}
          {table.isView && <span className="view-badge">View</span>}
        </div>
      </div>

      <div className="usage-tabs">
        <div className="tab-navigation">
          <button
            className={`tab-btn ${activeTab === 'source' ? 'active' : ''}`}
            onClick={() => setActiveTab('source')}
          >
            📥 Used as Source ({sourceUsage.length} groups, {sourceUsage.reduce((sum, group) => sum + group.statements.length, 0)} statements)
          </button>
          <button
            className={`tab-btn ${activeTab === 'target' ? 'active' : ''}`}
            onClick={() => setActiveTab('target')}
          >
            📤 Used as Target ({targetUsage.length} groups, {targetUsage.reduce((sum, group) => sum + group.statements.length, 0)} statements)
          </button>
        </div>

        <div className="tab-content active">
          {activeTab === 'source' && (
            <div className="usage-content">
              {sourceUsage.length > 0 ? (
                <div className="script-groups">
                  {sourceUsage.map(renderScriptGroup)}
                </div>
              ) : (
                <p className="no-data">No statements use this table as a source</p>
              )}
            </div>
          )}

          {activeTab === 'target' && (
            <div className="usage-content">
              {targetUsage.length > 0 ? (
                <div className="script-groups">
                  {targetUsage.map(renderScriptGroup)}
                </div>
              ) : (
                <p className="no-data">No statements use this table as a target</p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default memo(TableUsageDetails);

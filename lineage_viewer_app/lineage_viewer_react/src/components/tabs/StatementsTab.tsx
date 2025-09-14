import React, { useState, useRef, useEffect } from 'react';
import { LineageData, Statement } from '../../types/LineageData';
import ScriptStatisticsModal from '../ScriptStatisticsModal';
import LoadingMessage from '../LoadingMessage';

interface StatementsTabProps {
  data: LineageData;
}

interface ExpandedScripts {
  [scriptName: string]: boolean;
}

const StatementsTab: React.FC<StatementsTabProps> = ({ data }) => {
  const [selectedScript, setSelectedScript] = useState<string | null>(null);
  const [selectedStatementIndex, setSelectedStatementIndex] = useState<number | null>(null);
  const [sidebarWidth, setSidebarWidth] = useState(300);
  const [isResizing, setIsResizing] = useState(false);
  const [expandedScripts, setExpandedScripts] = useState<ExpandedScripts>({});
  const [filterText, setFilterText] = useState('');
  const [showScriptStatistics, setShowScriptStatistics] = useState(false);
  const [selectedScriptForStats, setSelectedScriptForStats] = useState<string | null>(null);
  const resizeHandleRef = useRef<HTMLDivElement>(null);
  const statementPanelRef = useRef<HTMLDivElement>(null);

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsResizing(true);
    e.preventDefault();
  };

  const handleMouseMove = (e: MouseEvent) => {
    if (!isResizing) return;
    
    const newWidth = e.clientX;
    if (newWidth >= 200 && newWidth <= 600) {
      setSidebarWidth(newWidth);
    }
  };

  const handleMouseUp = () => {
    setIsResizing(false);
  };

  useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing]);

  // Helper function to clean up script names for display
  const cleanScriptName = (scriptName: string): string => {
    let displayName = scriptName.split('/').pop() || scriptName;
    displayName = displayName.replace(/\.json$/i, '');
    
    if (displayName.match(/_sh_lineage$/i)) {
      displayName = displayName.replace(/_sh_lineage$/i, '.sh');
    } else if (displayName.match(/_ksh_lineage$/i)) {
      displayName = displayName.replace(/_ksh_lineage$/i, '.ksh');
    } else if (displayName.match(/_sql_lineage$/i)) {
      displayName = displayName.replace(/_sql_lineage$/i, '.sql');
    } else if (displayName.match(/_lineage$/i)) {
      displayName = displayName.replace(/_lineage$/i, '');
    }
    
    return displayName;
  };

  // Toggle script expansion
  const toggleScript = (scriptName: string) => {
    setExpandedScripts(prev => ({
      ...prev,
      [scriptName]: !prev[scriptName]
    }));
  };

  // Handle statement selection by script and index
  const handleStatementClick = (scriptName: string, statementIndex: number) => {
    setSelectedScript(scriptName);
    setSelectedStatementIndex(statementIndex);
  };

  // Auto-scroll to selected statement when it changes
  useEffect(() => {
    if (selectedStatementIndex !== null && statementPanelRef.current) {
      const selectedElement = statementPanelRef.current.querySelector(`[data-statement-index="${selectedStatementIndex}"]`);
      if (selectedElement) {
        selectedElement.scrollIntoView({ 
          behavior: 'smooth', 
          block: 'center' 
        });
      }
    }
  }, [selectedStatementIndex]);

  // Add null check to prevent runtime errors
  if (!data || !data.scripts) {
    return (
      <div className="main-content">
        <div className="sidebar" style={{ width: 300 }}>
          <h3>🔧 SQL Statements</h3>
          <ul className="statement-list">
            <li className="statement-item" style={{ color: '#6c757d', fontStyle: 'italic' }}>
              No data loaded
            </li>
          </ul>
        </div>
        <div className="resize-handle" style={{ left: 300 }} />
        <div className="content-area">
          <LoadingMessage />
        </div>
      </div>
    );
  }

  // Get statements for the selected script
  const getSelectedScriptStatements = () => {
    if (!selectedScript || !data.scripts) return [];
    const scriptData = data.scripts[selectedScript];
    return scriptData?.bteq_statements || [];
  };

  // Filter scripts based on search text
  const getFilteredScripts = () => {
    if (!data.scripts) return [];
    
    if (!filterText.trim()) {
      return Object.keys(data.scripts);
    }
    
    const searchText = filterText.toLowerCase();
    return Object.keys(data.scripts).filter(scriptName => {
      const displayName = cleanScriptName(scriptName).toLowerCase();
      return displayName.includes(searchText);
    });
  };

  // Handle filter input change
  const handleFilterChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFilterText(e.target.value);
  };

  // Clear filter
  const clearFilter = () => {
    setFilterText('');
  };

  // Handle script statistics button click
  const handleScriptStatisticsClick = (scriptName: string, e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent script expansion
    setSelectedScriptForStats(scriptName);
    setShowScriptStatistics(true);
  };

  return (
    <div className="main-content">
      <div 
        className="sidebar" 
        style={{ width: sidebarWidth }}
      >
        <h3 style={{ fontSize: '1.1em', marginBottom: '12px' }}>🔧 SQL Statements ({data.scripts ? Object.values(data.scripts).reduce((total, script) => total + (script.bteq_statements?.length || 0), 0) : 0})</h3>
        
        {/* Filter Input */}
        <div className="statement-filter-container" style={{ marginBottom: '16px' }}>
          <div style={{ position: 'relative' }}>
            <input
              type="text"
              placeholder="Filter scripts..."
              value={filterText}
              onChange={handleFilterChange}
              style={{
                width: '100%',
                padding: '8px 12px',
                border: '1px solid #ccc',
                borderRadius: '4px',
                fontSize: '0.9em',
                paddingRight: filterText ? '32px' : '12px'
              }}
            />
            {filterText && (
              <button
                onClick={clearFilter}
                style={{
                  position: 'absolute',
                  right: '8px',
                  top: '50%',
                  transform: 'translateY(-50%)',
                  background: 'none',
                  border: 'none',
                  cursor: 'pointer',
                  fontSize: '16px',
                  color: '#6c757d',
                  padding: '0',
                  width: '20px',
                  height: '20px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center'
                }}
                title="Clear filter"
              >
                ×
              </button>
            )}
          </div>
        </div>
        
        <div className="tree-view">
          {data.scripts && getFilteredScripts().length > 0 ? (
            getFilteredScripts().map(scriptName => {
              const scriptData = data.scripts![scriptName];
              const statements = scriptData.bteq_statements || [];
              if (statements.length === 0) return null;

              const displayScriptName = cleanScriptName(scriptName);
              const isExpanded = expandedScripts[scriptName];

              return (
                <div key={scriptName} className="file-group">
                  <div 
                    className={`tree-toggle ${isExpanded ? 'expanded' : ''}`}
                    onClick={() => toggleScript(scriptName)}
                  >
                    <span className="toggle-icon">▶</span>
                    <span>📄 {displayScriptName} ({statements.length})</span>
                  </div>
                  <div className={`tree-children ${isExpanded ? 'expanded' : ''}`}>
                    {statements.map((statement: string, localIndex: number) => {
                      const firstLine = statement.split('\n')[0].trim();
                      const displayText = firstLine.length > 60 ? firstLine.substring(0, 60) + '...' : firstLine;
                      const isSelected = selectedScript === scriptName && selectedStatementIndex === localIndex;

                      return (
                        <div
                          key={`${scriptName}-${localIndex}`}
                          className={`tree-item ${isSelected ? 'selected' : ''}`}
                          onClick={() => handleStatementClick(scriptName, localIndex)}
                        >
                          <div style={{ fontWeight: 'bold' }}>
                            Statement {displayScriptName}:{localIndex + 1}
                          </div>
                          <div style={{ fontSize: '0.75em', color: '#6c757d' }}>
                            {displayText}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })
          ) : (
            <div style={{ 
              padding: '20px', 
              textAlign: 'center', 
              color: '#6c757d', 
              fontStyle: 'italic',
              fontSize: '0.9em'
            }}>
              {filterText ? 'No scripts found matching your filter' : 'No scripts available'}
            </div>
          )}
        </div>
      </div>
      
      <div 
        className="resize-handle"
        ref={resizeHandleRef}
        onMouseDown={handleMouseDown}
        style={{ left: sidebarWidth }}
      />
      
      <div className="content-area" ref={statementPanelRef}>
        {selectedScript ? (
          <div className="statement-panel">
            <div className="statement-panel-header">
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                <h3 style={{ margin: 0 }}>📄 {cleanScriptName(selectedScript)} - SQL Statements</h3>
                <button
                  className="script-stats-button"
                  onClick={(e) => handleScriptStatisticsClick(selectedScript, e)}
                  title="Show script statistics"
                >
                  📊 Stats
                </button>
              </div>
              <p>Click on a statement in the left panel to view its details</p>
            </div>
            <div className="statement-list-panel">
              {getSelectedScriptStatements().map((statement: string, index: number) => (
                <div
                  key={`${selectedScript}-${index}`}
                  className={`statement-panel-item ${selectedStatementIndex === index ? 'selected' : ''}`}
                  data-statement-index={index}
                >
                  <div className="statement-header">
                    <span className="statement-number">Statement {index + 1}</span>
                    <span className="statement-line">Line {index + 1}</span>
                  </div>
                  <div className="statement-content">
                    <pre>{statement}</pre>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div className="loading">
            <h3>📁 Select a script to view its SQL statements</h3>
            <p>Click on a script in the left panel to see all its SQL statements</p>
          </div>
        )}
      </div>
      
      {/* Script Statistics Modal */}
      {showScriptStatistics && selectedScriptForStats && (
        <ScriptStatisticsModal
          data={data}
          scriptName={selectedScriptForStats}
          onClose={() => {
            setShowScriptStatistics(false);
            setSelectedScriptForStats(null);
          }}
        />
      )}
    </div>
  );
};

export default StatementsTab;

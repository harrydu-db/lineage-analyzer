import React, { useState, useMemo } from 'react';
import { LineageData } from '../types/LineageData';

interface ScriptStatisticsModalProps {
  data: LineageData;
  scriptName: string;
  onClose: () => void;
}

interface ScriptStats {
  totalTables: number;
  totalEdges: number;
  sourceTables: number;
  finalTargetTables: number;
  intermediateTables: number;
  unusedVolatileTables: number;
  volatileTables: number;
  globalTables: number;
  unusedVolatileTableDetails: Array<{ name: string; owners: string[] }>;
  sourceTableDetails: Array<{ name: string; owners: string[] }>;
  finalTargetTableDetails: Array<{ name: string; owners: string[] }>;
  intermediateTableDetails: Array<{ name: string; owners: string[] }>;
  volatileTableDetails: Array<{ name: string; owners: string[] }>;
  globalTableDetails: Array<{ name: string; owners: string[] }>;
}

const ScriptStatisticsModal: React.FC<ScriptStatisticsModalProps> = ({ 
  data, 
  scriptName, 
  onClose 
}) => {
  const [expandedSections, setExpandedSections] = useState<{ [key: string]: boolean }>({});
  const [selectedDetailType, setSelectedDetailType] = useState<string | null>(null);

  // Calculate script-specific statistics
  const stats: ScriptStats = useMemo(() => {
    if (!data || !data.scripts || !data.scripts[scriptName]) {
      return {
        totalTables: 0,
        totalEdges: 0,
        sourceTables: 0,
        finalTargetTables: 0,
        intermediateTables: 0,
        unusedVolatileTables: 0,
        volatileTables: 0,
        globalTables: 0,
        unusedVolatileTableDetails: [],
        sourceTableDetails: [],
        finalTargetTableDetails: [],
        intermediateTableDetails: [],
        volatileTableDetails: [],
        globalTableDetails: []
      };
    }

    // Build all nodes and edges from scripts data, filtered by script
    const allNodes: { [key: string]: any } = {};
    const allEdges: any[] = [];
    const edgeIds = new Set<string>();

    // PASS 1: Build all nodes with proper ownership (only for the selected script)
    const scriptData = data.scripts[scriptName];
    Object.entries((scriptData.tables || {})).forEach(([tableName, tableObj]: [string, any]) => {
      // Table names are already uppercase and global
      let currentNodeId: string;
      if (tableObj.is_volatile) {
        currentNodeId = `${scriptName}::${tableName}`;
      } else {
        currentNodeId = tableName;
      }

      if (!allNodes[currentNodeId]) {
        allNodes[currentNodeId] = {
          id: currentNodeId,
          name: tableName,
          is_volatile: tableObj.is_volatile,
          is_view: tableObj.is_view,
          owners: [scriptName],
          script: scriptName
        };
      } else {
        allNodes[currentNodeId].owners.push(scriptName);
        // Only merge properties for non-volatile tables (volatile tables are script-specific)
        if (!tableObj.is_volatile) {
          // If any definition has is_view: true, treat the table as a view
          if (tableObj.is_view) {
            allNodes[currentNodeId].is_view = true;
          }
        }
      }
    });

    // PASS 2: Create edges from relationships (only for the selected script)
    Object.entries((scriptData.tables || {})).forEach(([tableName, tableObj]: [string, any]) => {
      // Convert table name to uppercase
      const upperTableName = tableName.toUpperCase();
      let currentNodeId: string;
      if (tableObj.is_volatile) {
        currentNodeId = `${scriptName}::${upperTableName}`;
      } else {
        currentNodeId = upperTableName;
      }

      // Process source relationships
      if (tableObj.source) {
        tableObj.source.forEach((rel: any) => {
          const upperRelName = rel.name.toUpperCase();
          const sourceTableKey = `${scriptName}::${upperRelName}`;
          let sourceTable = scriptData.tables[rel.name];
          let sourceScript = scriptName;

          if (!sourceTable) {
            // Look for table in other scripts
            for (const [otherScriptName, otherScriptData] of Object.entries(data.scripts || {})) {
              if (otherScriptData.tables && otherScriptData.tables[rel.name]) {
                sourceTable = otherScriptData.tables[rel.name];
                sourceScript = otherScriptName;
                break;
              }
            }
          }

          let sourceNodeId: string;
          if (sourceTable && sourceTable.is_volatile) {
            sourceNodeId = `${sourceScript}::${upperRelName}`;
          } else {
            sourceNodeId = upperRelName;
          }

          if (!allNodes[sourceNodeId]) {
            allNodes[sourceNodeId] = {
              id: sourceNodeId,
              name: upperRelName,
              is_volatile: sourceTable ? sourceTable.is_volatile : false,
              owners: [sourceScript],
              script: sourceScript
            };
          }

          if (allNodes[sourceNodeId] && allNodes[currentNodeId] && rel.operation && rel.operation.length > 0) {
            const edgeId = `${sourceNodeId}-${currentNodeId}`;
            if (!edgeIds.has(edgeId)) {
              edgeIds.add(edgeId);
              const operations = rel.operation.map((opIndex: number) => `${scriptName}::op${opIndex}`);
              allEdges.push([sourceNodeId, currentNodeId, operations]);
            }
          }
        });
      }

      // Process target relationships
      if (tableObj.target) {
        tableObj.target.forEach((rel: any) => {
          const upperRelName = rel.name.toUpperCase();
          const targetTableKey = `${scriptName}::${upperRelName}`;
          let targetTable = scriptData.tables[rel.name];
          let targetScript = scriptName;

          if (!targetTable) {
            // Look for table in other scripts
            for (const [otherScriptName, otherScriptData] of Object.entries(data.scripts || {})) {
              if (otherScriptData.tables && otherScriptData.tables[rel.name]) {
                targetTable = otherScriptData.tables[rel.name];
                targetScript = otherScriptName;
                break;
              }
            }
          }

          let targetNodeId: string;
          if (targetTable && targetTable.is_volatile) {
            targetNodeId = `${targetScript}::${upperRelName}`;
          } else {
            targetNodeId = upperRelName;
          }

          if (!allNodes[targetNodeId]) {
            allNodes[targetNodeId] = {
              id: targetNodeId,
              name: upperRelName,
              is_volatile: targetTable ? targetTable.is_volatile : false,
              owners: [targetScript],
              script: targetScript
            };
          }

          if (allNodes[currentNodeId] && allNodes[targetNodeId] && rel.operation && rel.operation.length > 0) {
            const edgeId = `${currentNodeId}-${targetNodeId}`;
            if (!edgeIds.has(edgeId)) {
              edgeIds.add(edgeId);
              const operations = rel.operation.map((opIndex: number) => `${scriptName}::op${opIndex}`);
              allEdges.push([currentNodeId, targetNodeId, operations]);
            }
          }
        });
      }
    });

    // Calculate basic statistics
    const totalTables = Object.keys(allNodes).length;
    const totalEdges = allEdges.length;

    // Helper functions for edge analysis
    const getIncomingEdges = (nodeId: string, edges: any[]) => {
      return edges.filter(([from, to]) => to === nodeId);
    };

    const getOutgoingEdges = (nodeId: string, edges: any[]) => {
      return edges.filter(([from, to]) => from === nodeId);
    };

    // Find source tables (global tables with no incoming edges)
    const sourceTables = Object.values(allNodes).filter(node => {
      if (node.is_volatile) return false; // Exclude volatile tables
      const hasIncomingEdges = getIncomingEdges(node.id, allEdges).length > 0;
      return !hasIncomingEdges;
    });

    // Find final target tables (global tables with no outgoing edges or only self-referencing edges)
    const finalTargetTables = Object.values(allNodes).filter(node => {
      if (node.is_volatile) return false; // Exclude volatile tables
      const outgoingEdges = getOutgoingEdges(node.id, allEdges);
      
      // If no outgoing edges, it's a final table
      if (outgoingEdges.length === 0) {
        return true;
      }
      
      // If all outgoing edges are self-referencing (from === to), it's a final table
      const hasOnlySelfReferences = outgoingEdges.every(([from, to]) => from === to);
      return hasOnlySelfReferences;
    });

    // Find intermediate tables (global tables that are neither source nor final)
    const intermediateTables = Object.values(allNodes).filter(node => {
      if (node.is_volatile) return false; // Exclude volatile tables
      const hasIncomingEdges = getIncomingEdges(node.id, allEdges).length > 0;
      const outgoingEdges = getOutgoingEdges(node.id, allEdges);
      const hasOutgoingEdges = outgoingEdges.length > 0;
      const hasOnlySelfReferences = outgoingEdges.every(([from, to]) => from === to);
      
      // Intermediate table: has both incoming and outgoing edges (excluding self-references)
      return hasIncomingEdges && hasOutgoingEdges && !hasOnlySelfReferences;
    });

    // Find unused volatile tables (volatile tables with no targets)
    const unusedVolatileTables = Object.values(allNodes).filter(node => {
      if (!node.is_volatile) return false;
      const hasOutgoingEdges = getOutgoingEdges(node.id, allEdges).length > 0;
      return !hasOutgoingEdges;
    });

    // Calculate table types
    const volatileTables = Object.values(allNodes).filter(node => node.is_volatile);
    const globalTables = Object.values(allNodes).filter(node => !node.is_volatile);

    return {
      totalTables,
      totalEdges,
      sourceTables: sourceTables.length,
      finalTargetTables: finalTargetTables.length,
      intermediateTables: intermediateTables.length,
      unusedVolatileTables: unusedVolatileTables.length,
      volatileTables: volatileTables.length,
      globalTables: globalTables.length,
      unusedVolatileTableDetails: unusedVolatileTables.map(node => ({
        name: node.name,
        owners: node.owners
      })),
      sourceTableDetails: sourceTables.map(node => ({
        name: node.name,
        owners: node.owners
      })),
      finalTargetTableDetails: finalTargetTables.map(node => ({
        name: node.name,
        owners: node.owners
      })),
      intermediateTableDetails: intermediateTables.map(node => ({
        name: node.name,
        owners: node.owners
      })),
      volatileTableDetails: volatileTables.map(node => ({
        name: node.name,
        owners: node.owners
      })),
      globalTableDetails: globalTables.map(node => ({
        name: node.name,
        owners: node.owners
      }))
    };
  }, [data, scriptName]);

  const toggleSection = (sectionName: string) => {
    setExpandedSections(prev => ({
      ...prev,
      [sectionName]: !prev[sectionName]
    }));
  };

  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId);
    if (element) {
      element.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  const handleTileClick = (detailType: string) => {
    setSelectedDetailType(detailType);
    // Special case for total-tables - scroll to the first list section
    if (detailType === 'total-tables') {
      scrollToSection('unused-volatile-list');
    } else {
      scrollToSection(`${detailType}-list`);
    }
  };

  if (!data || !data.scripts || !data.scripts[scriptName]) {
    return null;
  }

  return (
    <div className="network-modal" onClick={onClose}>
      <div className="network-modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="network-modal-header">
          <h3>📊 Script Statistics - {scriptName}</h3>
          <button className="close-btn" onClick={onClose}>
            ✕ Close
          </button>
        </div>
        
        <div className="network-modal-content-body">
          {/* Statistics Grid - Excluding Total Connections, Avg Connections, and Included Scripts */}
          <div style={{ 
            display: 'grid', 
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
            gap: '20px', 
            marginBottom: '30px' 
          }}>
            <div 
              style={{ 
                padding: '20px', 
                backgroundColor: 'white', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6',
                textAlign: 'center',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                transform: selectedDetailType === 'total-tables' ? 'scale(1.02)' : 'scale(1)',
                borderColor: selectedDetailType === 'total-tables' ? '#007bff' : '#dee2e6'
              }}
              onClick={() => handleTileClick('total-tables')}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'scale(1.02)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                if (selectedDetailType !== 'total-tables') {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }
              }}
            >
              <h4 style={{ color: '#007bff', margin: '0 0 10px 0' }}>TOTAL TABLES</h4>
              <div style={{ fontSize: '2.5em', fontWeight: 'bold', color: '#1976d2' }}>
                {stats.totalTables}
              </div>
              <div style={{ color: '#6c757d', fontSize: '0.9em' }}>Tables in this script</div>
            </div>

            <div 
              style={{ 
                padding: '20px', 
                backgroundColor: 'white', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6',
                textAlign: 'center',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                transform: selectedDetailType === 'source-tables' ? 'scale(1.02)' : 'scale(1)',
                borderColor: selectedDetailType === 'source-tables' ? '#ffc107' : '#dee2e6'
              }}
              onClick={() => handleTileClick('source-tables')}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'scale(1.02)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                if (selectedDetailType !== 'source-tables') {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }
              }}
            >
              <h4 style={{ color: '#ffc107', margin: '0 0 10px 0' }}>SOURCE TABLES</h4>
              <div style={{ fontSize: '2.5em', fontWeight: 'bold', color: '#ffc107' }}>
                {stats.sourceTables}
              </div>
              <div style={{ color: '#6c757d', fontSize: '0.9em' }}>Tables with no incoming data</div>
            </div>

            <div 
              style={{ 
                padding: '20px', 
                backgroundColor: 'white', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6',
                textAlign: 'center',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                transform: selectedDetailType === 'final-tables' ? 'scale(1.02)' : 'scale(1)',
                borderColor: selectedDetailType === 'final-tables' ? '#dc3545' : '#dee2e6'
              }}
              onClick={() => handleTileClick('final-tables')}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'scale(1.02)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                if (selectedDetailType !== 'final-tables') {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }
              }}
            >
              <h4 style={{ color: '#dc3545', margin: '0 0 10px 0' }}>FINAL TABLES</h4>
              <div style={{ fontSize: '2.5em', fontWeight: 'bold', color: '#dc3545' }}>
                {stats.finalTargetTables}
              </div>
              <div style={{ color: '#6c757d', fontSize: '0.9em' }}>Tables with no outgoing data or only self-references</div>
            </div>

            <div 
              style={{ 
                padding: '20px', 
                backgroundColor: 'white', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6',
                textAlign: 'center',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                transform: selectedDetailType === 'intermediate-tables' ? 'scale(1.02)' : 'scale(1)',
                borderColor: selectedDetailType === 'intermediate-tables' ? '#17a2b8' : '#dee2e6'
              }}
              onClick={() => handleTileClick('intermediate-tables')}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'scale(1.02)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                if (selectedDetailType !== 'intermediate-tables') {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }
              }}
            >
              <h4 style={{ color: '#17a2b8', margin: '0 0 10px 0' }}>INTERMEDIATE TABLES</h4>
              <div style={{ fontSize: '2.5em', fontWeight: 'bold', color: '#17a2b8' }}>
                {stats.intermediateTables}
              </div>
              <div style={{ color: '#6c757d', fontSize: '0.9em' }}>Global tables with both incoming and outgoing data</div>
            </div>

            <div 
              style={{ 
                padding: '20px', 
                backgroundColor: 'white', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6',
                textAlign: 'center',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                transform: selectedDetailType === 'volatile-tables' ? 'scale(1.02)' : 'scale(1)',
                borderColor: selectedDetailType === 'volatile-tables' ? '#fd7e14' : '#dee2e6'
              }}
              onClick={() => handleTileClick('volatile-tables')}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'scale(1.02)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                if (selectedDetailType !== 'volatile-tables') {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }
              }}
            >
              <h4 style={{ color: '#fd7e14', margin: '0 0 10px 0' }}>VOLATILE TABLES</h4>
              <div style={{ fontSize: '2.5em', fontWeight: 'bold', color: '#fd7e14' }}>
                {stats.volatileTables}
              </div>
              <div style={{ color: '#6c757d', fontSize: '0.9em' }}>Script-specific tables</div>
            </div>

            <div 
              style={{ 
                padding: '20px', 
                backgroundColor: 'white', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6',
                textAlign: 'center',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                transform: selectedDetailType === 'global-tables' ? 'scale(1.02)' : 'scale(1)',
                borderColor: selectedDetailType === 'global-tables' ? '#6f42c1' : '#dee2e6'
              }}
              onClick={() => handleTileClick('global-tables')}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'scale(1.02)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                if (selectedDetailType !== 'global-tables') {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }
              }}
            >
              <h4 style={{ color: '#6f42c1', margin: '0 0 10px 0' }}>GLOBAL TABLES</h4>
              <div style={{ fontSize: '2.5em', fontWeight: 'bold', color: '#6f42c1' }}>
                {stats.globalTables}
              </div>
              <div style={{ color: '#6c757d', fontSize: '0.9em' }}>Shared across scripts</div>
            </div>

            <div 
              style={{ 
                padding: '20px', 
                backgroundColor: 'white', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6',
                textAlign: 'center',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                transform: selectedDetailType === 'unused-volatile' ? 'scale(1.02)' : 'scale(1)',
                borderColor: selectedDetailType === 'unused-volatile' ? '#e83e8c' : '#dee2e6'
              }}
              onClick={() => handleTileClick('unused-volatile')}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'scale(1.02)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={(e) => {
                if (selectedDetailType !== 'unused-volatile') {
                  e.currentTarget.style.transform = 'scale(1)';
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }
              }}
            >
              <h4 style={{ color: '#e83e8c', margin: '0 0 10px 0' }}>UNUSED VOLATILE</h4>
              <div style={{ fontSize: '2.5em', fontWeight: 'bold', color: '#e83e8c' }}>
                {stats.unusedVolatileTables}
              </div>
              <div style={{ color: '#6c757d', fontSize: '0.9em' }}>Should be removed</div>
            </div>
          </div>

          {/* Detailed List Sections - Same as NetworkStatistics but filtered for script */}
          <div style={{ marginBottom: '30px' }}>
            {/* Unused Volatile Tables List */}
            {stats.unusedVolatileTableDetails.length > 0 && (
              <div id="unused-volatile-list" style={{ 
                background: 'white', 
                padding: '20px', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6', 
                marginBottom: '20px',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
              }}>
                <div 
                  style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    alignItems: 'center', 
                    marginBottom: '15px', 
                    cursor: 'pointer' 
                  }}
                  onClick={() => toggleSection('unused-volatile-tables')}
                >
                  <h4 style={{ color: '#e83e8c', margin: 0 }}>
                    ⚠️ Unused Volatile Tables ({stats.unusedVolatileTableDetails.length})
                  </h4>
                  <span style={{ fontSize: '18px', color: '#6c757d' }}>
                    {expandedSections['unused-volatile-tables'] ? '▲' : '▼'}
                  </span>
                </div>
                {expandedSections['unused-volatile-tables'] && (
                  <div>
                    <p style={{ color: '#856404', marginBottom: '15px', fontSize: '0.9em' }}>
                      These volatile tables have no targets and should be removed from the scripts.
                    </p>
                    {stats.unusedVolatileTableDetails.map((table, index) => (
                      <div 
                        key={index}
                        style={{ 
                          display: 'flex', 
                          justifyContent: 'space-between', 
                          alignItems: 'center', 
                          padding: '8px 0', 
                          borderBottom: '1px solid #f0f0f0' 
                        }}
                      >
                        <span style={{ fontWeight: 'bold', color: '#495057' }}>{table.name}</span>
                        <span style={{ fontSize: '0.8em', color: '#6c757d', fontStyle: 'italic' }}>
                          {table.owners.join(', ')}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Source Tables List */}
            {stats.sourceTableDetails.length > 0 && (
              <div id="source-tables-list" style={{ 
                background: 'white', 
                padding: '20px', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6', 
                marginBottom: '20px',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
              }}>
                <div 
                  style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    alignItems: 'center', 
                    marginBottom: '15px', 
                    cursor: 'pointer' 
                  }}
                  onClick={() => toggleSection('source-tables-list')}
                >
                  <h4 style={{ color: '#ffc107', margin: 0 }}>
                    📥 Source Tables ({stats.sourceTableDetails.length})
                  </h4>
                  <span style={{ fontSize: '18px', color: '#6c757d' }}>
                    {expandedSections['source-tables-list'] ? '▲' : '▼'}
                  </span>
                </div>
                {expandedSections['source-tables-list'] && (
                  <div>
                    <p style={{ color: '#6c757d', marginBottom: '15px', fontSize: '0.9em' }}>
                      Global tables that provide data but don't receive data from other tables.
                    </p>
                    {stats.sourceTableDetails.map((table, index) => (
                      <div 
                        key={index}
                        style={{ 
                          display: 'flex', 
                          justifyContent: 'space-between', 
                          alignItems: 'center', 
                          padding: '8px 0', 
                          borderBottom: '1px solid #f0f0f0' 
                        }}
                      >
                        <span style={{ fontWeight: 'bold', color: '#495057' }}>{table.name}</span>
                        <div style={{ fontSize: '0.8em', color: '#6c757d', fontStyle: 'italic' }}>
                          {table.owners.join(', ')}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Final Tables List */}
            {stats.finalTargetTableDetails.length > 0 && (
              <div id="final-tables-list" style={{ 
                background: 'white', 
                padding: '20px', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6', 
                marginBottom: '20px',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
              }}>
                <div 
                  style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    alignItems: 'center', 
                    marginBottom: '15px', 
                    cursor: 'pointer' 
                  }}
                  onClick={() => toggleSection('final-tables-list')}
                >
                  <h4 style={{ color: '#dc3545', margin: 0 }}>
                    📤 Final Tables ({stats.finalTargetTableDetails.length})
                  </h4>
                  <span style={{ fontSize: '18px', color: '#6c757d' }}>
                    {expandedSections['final-tables-list'] ? '▲' : '▼'}
                  </span>
                </div>
                {expandedSections['final-tables-list'] && (
                  <div>
                    <p style={{ color: '#6c757d', marginBottom: '15px', fontSize: '0.9em' }}>
                      Tables that receive data but don't provide data to other tables, or only reference themselves.
                    </p>
                    {stats.finalTargetTableDetails.map((table, index) => (
                      <div 
                        key={index}
                        style={{ 
                          display: 'flex', 
                          justifyContent: 'space-between', 
                          alignItems: 'center', 
                          padding: '8px 0', 
                          borderBottom: '1px solid #f0f0f0' 
                        }}
                      >
                        <span style={{ fontWeight: 'bold', color: '#495057' }}>{table.name}</span>
                        <div style={{ fontSize: '0.8em', color: '#6c757d', fontStyle: 'italic' }}>
                          {table.owners.join(', ')}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Intermediate Tables List */}
            {stats.intermediateTableDetails.length > 0 && (
              <div id="intermediate-tables-list" style={{ 
                background: 'white', 
                padding: '20px', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6', 
                marginBottom: '20px',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
              }}>
                <div 
                  style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    alignItems: 'center', 
                    marginBottom: '15px', 
                    cursor: 'pointer' 
                  }}
                  onClick={() => toggleSection('intermediate-tables-list')}
                >
                  <h4 style={{ color: '#17a2b8', margin: 0 }}>
                    🔄 Intermediate Tables ({stats.intermediateTableDetails.length})
                  </h4>
                  <span style={{ fontSize: '18px', color: '#6c757d' }}>
                    {expandedSections['intermediate-tables-list'] ? '▲' : '▼'}
                  </span>
                </div>
                {expandedSections['intermediate-tables-list'] && (
                  <div>
                    <p style={{ color: '#6c757d', marginBottom: '15px', fontSize: '0.9em' }}>
                      Global tables that have both incoming and outgoing data connections. These are intermediate processing tables.
                    </p>
                    {stats.intermediateTableDetails.map((table, index) => (
                      <div 
                        key={index}
                        style={{ 
                          display: 'flex', 
                          justifyContent: 'space-between', 
                          alignItems: 'center', 
                          padding: '8px 0', 
                          borderBottom: '1px solid #f0f0f0' 
                        }}
                      >
                        <span style={{ fontWeight: 'bold', color: '#495057' }}>{table.name}</span>
                        <div style={{ fontSize: '0.8em', color: '#6c757d', fontStyle: 'italic' }}>
                          {table.owners.join(', ')}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Volatile Tables List */}
            {stats.volatileTableDetails.length > 0 && (
              <div id="volatile-tables-list" style={{ 
                background: 'white', 
                padding: '20px', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6', 
                marginBottom: '20px',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
              }}>
                <div 
                  style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    alignItems: 'center', 
                    marginBottom: '15px', 
                    cursor: 'pointer' 
                  }}
                  onClick={() => toggleSection('volatile-tables-list')}
                >
                  <h4 style={{ color: '#fd7e14', margin: 0 }}>
                    ⚡ Volatile Tables ({stats.volatileTableDetails.length})
                  </h4>
                  <span style={{ fontSize: '18px', color: '#6c757d' }}>
                    {expandedSections['volatile-tables-list'] ? '▲' : '▼'}
                  </span>
                </div>
                {expandedSections['volatile-tables-list'] && (
                  <div>
                    <p style={{ color: '#6c757d', marginBottom: '15px', fontSize: '0.9em' }}>
                      Script-specific temporary tables. These are created and destroyed within individual scripts.
                    </p>
                    {stats.volatileTableDetails.map((table, index) => (
                      <div 
                        key={index}
                        style={{ 
                          display: 'flex', 
                          justifyContent: 'space-between', 
                          alignItems: 'center', 
                          padding: '8px 0', 
                          borderBottom: '1px solid #f0f0f0' 
                        }}
                      >
                        <div>
                          <span style={{ fontWeight: 'bold', color: '#495057' }}>{table.name}</span>
                          <span style={{ 
                            background: '#ffc107', 
                            color: '#856404', 
                            padding: '2px 6px', 
                            borderRadius: '4px', 
                            fontSize: '0.7em', 
                            marginLeft: '8px' 
                          }}>
                            VOLATILE
                          </span>
                        </div>
                        <div style={{ fontSize: '0.8em', color: '#6c757d', fontStyle: 'italic' }}>
                          {table.owners.join(', ')}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Global Tables List */}
            {stats.globalTableDetails.length > 0 && (
              <div id="global-tables-list" style={{ 
                background: 'white', 
                padding: '20px', 
                borderRadius: '8px', 
                border: '1px solid #dee2e6', 
                marginBottom: '20px',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
              }}>
                <div 
                  style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    alignItems: 'center', 
                    marginBottom: '15px', 
                    cursor: 'pointer' 
                  }}
                  onClick={() => toggleSection('global-tables-list')}
                >
                  <h4 style={{ color: '#6f42c1', margin: 0 }}>
                    🌐 Global Tables ({stats.globalTableDetails.length})
                  </h4>
                  <span style={{ fontSize: '18px', color: '#6c757d' }}>
                    {expandedSections['global-tables-list'] ? '▲' : '▼'}
                  </span>
                </div>
                {expandedSections['global-tables-list'] && (
                  <div>
                    <p style={{ color: '#6c757d', marginBottom: '15px', fontSize: '0.9em' }}>
                      Tables shared across multiple scripts. These are persistent and can be referenced by any script.
                    </p>
                    {stats.globalTableDetails.map((table, index) => (
                      <div 
                        key={index}
                        style={{ 
                          display: 'flex', 
                          justifyContent: 'space-between', 
                          alignItems: 'center', 
                          padding: '8px 0', 
                          borderBottom: '1px solid #f0f0f0'
                        }}
                      >
                        <span style={{ fontWeight: 'bold', color: '#495057' }}>{table.name}</span>
                        <div style={{ fontSize: '0.8em', color: '#6c757d', fontStyle: 'italic' }}>
                          {table.owners.join(', ')}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ScriptStatisticsModal;

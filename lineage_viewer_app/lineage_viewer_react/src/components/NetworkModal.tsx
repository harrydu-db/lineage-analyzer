import React from 'react';
import { NetworkEdge, LineageData } from '../types/LineageData';

interface NetworkModalProps {
  edge: NetworkEdge;
  data: LineageData;
  onClose: () => void;
}

const NetworkModal: React.FC<NetworkModalProps> = ({ edge, data, onClose }) => {
  // Parse operations from edge label (format: "ScriptName:Index|Index|Index, ScriptName2:Index")
  const parseOperations = (label: string) => {
    const opGroups: { scriptName: string; indices: number[] }[] = [];
    
    if (!label) return opGroups;
    
    label.split(',').forEach(part => {
      const trimmed = part.trim();
      const match = trimmed.match(/^(.*?):([\d|]+)$/);
      if (match) {
        const scriptName = match[1];
        const indices = match[2].split('|').map(x => parseInt(x, 10)).filter(x => !isNaN(x));
        if (indices.length > 0) {
          opGroups.push({ scriptName, indices });
        }
      }
    });
    
    return opGroups;
  };

  // Get SQL statements for the operations
  const getSqlStatements = (opGroups: { scriptName: string; indices: number[] }[]) => {
    const statements: { scriptName: string; index: number; sql: string }[] = [];
    
    opGroups.forEach(group => {
      let scriptData: any = null;
      
      // Find script data
      if (data.scripts && data.scripts[group.scriptName]) {
        scriptData = data.scripts[group.scriptName];
      } else if (data.scripts) {
        // Try to find by normalized script name
        for (const [key, sData] of Object.entries(data.scripts)) {
          let normKey = key.split('/').pop()?.replace(/\.json$/i, '') || '';
          if (normKey.match(/_sh_lineage$/i)) normKey = normKey.replace(/_sh_lineage$/i, '.sh');
          else if (normKey.match(/_ksh_lineage$/i)) normKey = normKey.replace(/_ksh_lineage$/i, '.ksh');
          else if (normKey.match(/_sql_lineage$/i)) normKey = normKey.replace(/_sql_lineage$/i, '.sql');
          else if (normKey.match(/_lineage$/i)) normKey = normKey.replace(/_lineage$/i, '');
          if (normKey === group.scriptName) {
            scriptData = sData;
            break;
          }
        }
      }
      
      // Get SQL statements for each index
      group.indices.forEach(localIdx => {
        let sqlStatement: string | null = null;
        if (scriptData && scriptData.bteq_statements && scriptData.bteq_statements.length > localIdx) {
          sqlStatement = scriptData.bteq_statements[localIdx];
        }
        
        if (sqlStatement) {
          statements.push({
            scriptName: group.scriptName,
            index: localIdx,
            sql: sqlStatement
          });
        }
      });
    });
    
    return statements;
  };

  const opGroups = parseOperations(edge.label || '');
  const sqlStatements = getSqlStatements(opGroups);

  return (
    <div className="network-modal" onClick={onClose}>
      <div className="network-modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="network-modal-header">
          <h3>Edge Details</h3>
          <button className="close-btn" onClick={onClose}>
            ✕ Close
          </button>
        </div>
        <div className="network-modal-content-body">
          <div style={{ marginBottom: '20px' }}>
            <h4 style={{ color: '#495057', marginBottom: '10px' }}>Data Flow</h4>
            <p><strong>From:</strong> {edge.from}</p>
            <p><strong>To:</strong> {edge.to}</p>
            <p><strong>Operations:</strong> {edge.label || 'UNKNOWN'}</p>
          </div>
          
          <div style={{ borderTop: '1px solid #dee2e6', paddingTop: '20px' }}>
            <h4 style={{ color: '#495057', marginBottom: '15px' }}>SQL Statements</h4>
            
            {sqlStatements.length > 0 ? (
              sqlStatements.map((stmt, index) => (
                <div key={`${stmt.scriptName}-${stmt.index}`} style={{
                  background: 'white',
                  padding: '15px',
                  borderRadius: '8px',
                  marginBottom: '15px',
                  border: '1px solid #dee2e6'
                }}>
                  <h5 style={{ 
                    color: '#007bff', 
                    marginBottom: '10px',
                    cursor: 'pointer'
                  }}>
                    {stmt.scriptName}:{stmt.index}
                  </h5>
                  <div style={{
                    background: '#f8f9fa',
                    padding: '12px',
                    borderRadius: '6px',
                    fontFamily: 'Courier New, monospace',
                    whiteSpace: 'pre-wrap',
                    fontSize: '12px',
                    lineHeight: '1.4',
                    maxHeight: '200px',
                    overflowY: 'auto',
                    border: '1px solid #e9ecef'
                  }}>
                    {stmt.sql}
                  </div>
                </div>
              ))
            ) : (
              <div style={{ 
                padding: '15px', 
                backgroundColor: '#f8f9fa', 
                borderRadius: '8px',
                border: '1px solid #dee2e6'
              }}>
                <p style={{ margin: 0, color: '#6c757d' }}>
                  No SQL statements found for this connection.
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default NetworkModal;


import React, { useState, useRef, useEffect, useMemo, useCallback, memo } from 'react';
import { LineageData, Table } from '../../types/LineageData';
import TableUsageDetails from '../TableUsageDetails';
import VirtualizedTableList from '../VirtualizedTableList';
import LoadingMessage from '../LoadingMessage';

interface TablesTabProps {
  data: LineageData;
  selectedTable: string | null;
  onTableSelect: (tableName: string | null) => void;
}

const TablesTab: React.FC<TablesTabProps> = ({ data, selectedTable, onTableSelect }) => {
  const [sidebarWidth, setSidebarWidth] = useState(400);
  const [isResizing, setIsResizing] = useState(false);
  const [filterText, setFilterText] = useState('');
  const resizeHandleRef = useRef<HTMLDivElement>(null);

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

  // Handle filter input change with useCallback
  const handleFilterChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setFilterText(e.target.value);
  }, []);

  // Clear filter with useCallback
  const clearFilter = useCallback(() => {
    setFilterText('');
  }, []);

  // Get filtered tables with useMemo for performance
  const filteredTables = useMemo(() => {
    if (!data || !data.tables) return [];
    
    // For volatile tables: keep separate entries per script (not global)
    // For non-volatile tables: deduplicate by table name (global)
    const uniqueTables = new Map();
    Object.values(data.tables).forEach(table => {
      let key: string;
      if (table.isVolatile) {
        // Volatile tables are script-specific, use script::table as key
        key = table.script ? `${table.script}::${table.name}` : table.name;
      } else {
        // Non-volatile tables are global, use just table name as key
        key = table.name;
      }
      
      if (!uniqueTables.has(key)) {
        uniqueTables.set(key, table);
      }
    });
    
    const allTables = Array.from(uniqueTables.values());
    
    // Sort tables alphabetically by name
    allTables.sort((a, b) => a.name.localeCompare(b.name));
    
    if (!filterText.trim()) {
      return allTables;
    }
    
    const searchText = filterText.toLowerCase();
    return allTables.filter(table => {
      const tableName = table.name.toLowerCase();
      const scriptName = table.script ? table.script.toLowerCase() : '';
      return tableName.includes(searchText) || scriptName.includes(searchText);
    });
  }, [data, filterText]);

  // Add null check to prevent runtime errors
  if (!data || !data.tables) {
    return (
      <div className="main-content">
        <div className="sidebar" style={{ width: 300 }}>
          <h3>📋 Tables</h3>
          <ul className="table-list">
            <li className="table-item" style={{ color: '#6c757d', fontStyle: 'italic' }}>
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


  return (
    <div className="main-content">
      <div 
        className="sidebar" 
        style={{ width: sidebarWidth }}
      >
        <h3>📋 Tables ({filteredTables.length})</h3>
        <div style={{ position: 'relative', marginBottom: '10px' }}>
          <input
            type="text"
            placeholder="Filter tables..."
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
                color: '#666'
              }}
            >
              ×
            </button>
          )}
        </div>
        {filteredTables.length > 0 ? (
          <div style={{ 
            flex: 1, 
            overflow: 'auto', 
            minHeight: 0,
            maxHeight: 'calc(100vh - 200px)',
            border: '1px solid #eee',
            borderRadius: '4px'
          }}>
            {filteredTables.map((table) => {
              // Use the same key logic as in the deduplication
              const tableKey = table.isVolatile && table.script 
                ? `${table.script}::${table.name}` 
                : table.name;
              return (
                <div
                  key={tableKey}
                  style={{
                    height: 40,
                    display: 'flex',
                    alignItems: 'center',
                    padding: '0 12px',
                    cursor: 'pointer',
                    borderBottom: '1px solid #eee'
                  }}
                  className={`table-item ${table.isVolatile ? 'volatile' : ''} ${table.isView ? 'view' : ''} ${
                    selectedTable === tableKey ? 'selected' : ''
                  }`}
                  onClick={() => onTableSelect(tableKey)}
                >
                  <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ fontSize: '0.85em' }}>{table.name}</span>
                    {table.script && table.isVolatile && (
                      <span className="script-badge" style={{ fontSize: '0.75em' }}>{table.script}</span>
                    )}
                    {table.isVolatile && <span className="volatile-badge" style={{ fontSize: '0.75em' }}>Volatile</span>}
                    {table.isView && <span className="view-badge" style={{ fontSize: '0.75em' }}>View</span>}
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="table-item" style={{ 
            color: '#6c757d', 
            fontStyle: 'italic',
            fontSize: '0.9em',
            padding: '20px',
            textAlign: 'center'
          }}>
            {filterText ? 'No tables found matching your filter' : 'No tables available'}
          </div>
        )}
      </div>
      
      <div 
        className="resize-handle"
        ref={resizeHandleRef}
        onMouseDown={handleMouseDown}
        style={{ left: sidebarWidth }}
      />
      
      <div className="content-area">
        {selectedTable ? (
          <TableUsageDetails 
            table={filteredTables.find(t => {
              const tableKey = t.isVolatile && t.script ? `${t.script}::${t.name}` : t.name;
              return tableKey === selectedTable;
            }) || Object.values(data.tables).find(t => {
              const tableKey = t.isVolatile && t.script ? `${t.script}::${t.name}` : t.name;
              return tableKey === selectedTable;
            })}
            data={data}
          />
        ) : (
          <LoadingMessage />
        )}
      </div>
    </div>
  );
};

export default memo(TablesTab);

import React, { useMemo, useCallback } from 'react';
import { Table } from '../types/LineageData';

interface VirtualizedTableListProps {
  tables: Table[];
  selectedTable: string | null;
  onTableSelect: (tableName: string | null) => void;
  containerHeight?: number | string;
  itemHeight?: number;
}

const VirtualizedTableList: React.FC<VirtualizedTableListProps> = ({
  tables,
  selectedTable,
  onTableSelect,
  containerHeight = 400,
  itemHeight = 50
}) => {
  const [scrollTop, setScrollTop] = React.useState(0);
  
  const visibleRange = useMemo(() => {
    const startIndex = Math.floor(scrollTop / itemHeight);
    const height = typeof containerHeight === 'string' ? 400 : containerHeight;
    const endIndex = Math.min(
      startIndex + Math.ceil(height / itemHeight) + 1,
      tables.length
    );
    return { startIndex, endIndex };
  }, [scrollTop, itemHeight, containerHeight, tables.length]);

  const visibleTables = useMemo(() => {
    return tables.slice(visibleRange.startIndex, visibleRange.endIndex);
  }, [tables, visibleRange]);

  const totalHeight = tables.length * itemHeight;
  const offsetY = visibleRange.startIndex * itemHeight;

  const handleScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    setScrollTop(e.currentTarget.scrollTop);
  }, []);

  const handleTableClick = useCallback((tableKey: string) => {
    onTableSelect(tableKey);
  }, [onTableSelect]);

  return (
    <div
      style={{
        height: containerHeight,
        overflow: 'auto',
        position: 'relative',
        flex: 1,
        minHeight: 0
      }}
      onScroll={handleScroll}
    >
      <div style={{ height: totalHeight, position: 'relative' }}>
        <div
          style={{
            transform: `translateY(${offsetY}px)`,
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0
          }}
        >
          {visibleTables.map((table, index) => {
            const actualIndex = visibleRange.startIndex + index;
            const tableKey = table.script ? `${table.script}::${table.name}` : table.name;
            
            return (
              <div
                key={tableKey}
                style={{
                  height: itemHeight,
                  display: 'flex',
                  alignItems: 'center',
                  padding: '0 15px',
                  cursor: 'pointer',
                  borderBottom: '1px solid #eee'
                }}
                className={`table-item ${table.isVolatile ? 'volatile' : ''} ${table.isView ? 'view' : ''} ${
                  selectedTable === tableKey ? 'selected' : ''
                }`}
                onClick={() => handleTableClick(tableKey)}
              >
                <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span>{table.name}</span>
                  {table.script && table.isVolatile && (
                    <span className="script-badge">{table.script}</span>
                  )}
                  {table.isVolatile && <span className="volatile-badge">Volatile</span>}
                  {table.isView && <span className="view-badge">View</span>}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

export default VirtualizedTableList;

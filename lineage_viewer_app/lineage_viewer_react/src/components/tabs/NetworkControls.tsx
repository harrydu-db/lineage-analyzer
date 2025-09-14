import React, { useState, useEffect, useRef } from 'react';

interface NetworkControlsProps {
  selectedScript: string | null;
  onScriptChange: (script: string | null) => void;
  onScriptSearch?: (scriptName: string) => void;
  connectionMode: 'direct' | 'impacts_by' | 'impacted_by' | 'both';
  onConnectionModeChange: (mode: 'direct' | 'impacts_by' | 'impacted_by' | 'both') => void;
  lockViewEnabled: boolean;
  onLockViewToggle: (enabled: boolean) => void;
  flowViewEnabled: boolean;
  onFlowViewToggle: (enabled: boolean) => void;
  onShowStatistics: () => void;
  availableScripts?: string[];
  availableTables?: string[];
  onTableSearch?: (tableName: string) => void;
  selectedTableFilters?: string[];
  onClearAll?: () => void;
}

const NetworkControls: React.FC<NetworkControlsProps> = ({
  selectedScript,
  onScriptChange,
  onScriptSearch,
  connectionMode,
  onConnectionModeChange,
  lockViewEnabled,
  onLockViewToggle,
  flowViewEnabled,
  onFlowViewToggle,
  onShowStatistics,
  availableScripts = [],
  availableTables = [],
  onTableSearch,
  selectedTableFilters = [],
  onClearAll
}) => {
  const [scriptSearch, setScriptSearch] = useState('');
  const [tableSearch, setTableSearch] = useState('');
  const [filteredScripts, setFilteredScripts] = useState<string[]>([]);
  const [filteredTables, setFilteredTables] = useState<string[]>([]);
  const [selectedScriptIndex, setSelectedScriptIndex] = useState(-1);
  const [selectedTableIndex, setSelectedTableIndex] = useState(-1);
  const [showScriptDropdown, setShowScriptDropdown] = useState(false);
  const [showTableDropdown, setShowTableDropdown] = useState(false);
  
  const scriptInputRef = useRef<HTMLInputElement>(null);
  const tableInputRef = useRef<HTMLInputElement>(null);
  const scriptDropdownRef = useRef<HTMLDivElement>(null);
  const tableDropdownRef = useRef<HTMLDivElement>(null);

  // Filter scripts based on search input
  useEffect(() => {
    console.log('Script search effect:', { scriptSearch, availableScripts });
    if (scriptSearch.trim()) {
      const filtered = availableScripts.filter(script =>
        script.toLowerCase().includes(scriptSearch.toLowerCase())
      );
      console.log('Filtered scripts:', filtered);
      setFilteredScripts(filtered);
      
      // Only show dropdown if there are multiple options or if the search doesn't exactly match
      const exactMatch = availableScripts.find(script => 
        script.toLowerCase() === scriptSearch.toLowerCase()
      );
      setShowScriptDropdown(filtered.length > 0 && !exactMatch);
      setSelectedScriptIndex(-1);
    } else {
      setFilteredScripts([]);
      setShowScriptDropdown(false);
      setSelectedScriptIndex(-1);
    }
  }, [scriptSearch, availableScripts]);

  // Filter tables based on search input
  useEffect(() => {
    console.log('Table search effect:', { tableSearch, availableTables });
    if (tableSearch.trim()) {
      const filtered = availableTables.filter(table =>
        table.toLowerCase().includes(tableSearch.toLowerCase())
      );
      console.log('Filtered tables:', filtered);
      setFilteredTables(filtered);
      
      // Only show dropdown if there are multiple options or if the search doesn't exactly match
      const exactMatch = availableTables.find(table => 
        table.toLowerCase() === tableSearch.toLowerCase()
      );
      setShowTableDropdown(filtered.length > 0 && !exactMatch);
      setSelectedTableIndex(-1);
    } else {
      setFilteredTables([]);
      setShowTableDropdown(false);
      setSelectedTableIndex(-1);
    }
  }, [tableSearch, availableTables]);

  // Debug dropdown states
  useEffect(() => {
    console.log('Dropdown states:', { 
      showScriptDropdown, 
      filteredScripts: filteredScripts.length, 
      showTableDropdown, 
      filteredTables: filteredTables.length 
    });
    if (showScriptDropdown) {
      console.log('Script dropdown should be visible with items:', filteredScripts);
    }
  }, [showScriptDropdown, filteredScripts, showTableDropdown, filteredTables]);


  // Handle script input change
  const handleScriptInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setScriptSearch(value);
    // Only update the input text and show dropdown, don't apply filter
  };

  // Handle table input change
  const handleTableInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setTableSearch(value);
    // Only update the input text and show dropdown, don't apply filter
  };

  // Handle script keyboard navigation
  const handleScriptKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!showScriptDropdown) return;

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setSelectedScriptIndex(prev => 
          prev < filteredScripts.length - 1 ? prev + 1 : 0
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setSelectedScriptIndex(prev => 
          prev > 0 ? prev - 1 : filteredScripts.length - 1
        );
        break;
      case 'Enter':
        e.preventDefault();
        if (selectedScriptIndex >= 0) {
          selectScript(selectedScriptIndex);
        } else {
          handleScriptSearch();
        }
        break;
      case 'Escape':
        setShowScriptDropdown(false);
        setSelectedScriptIndex(-1);
        break;
    }
  };

  // Handle table keyboard navigation
  const handleTableKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!showTableDropdown) return;

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setSelectedTableIndex(prev => 
          prev < filteredTables.length - 1 ? prev + 1 : 0
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setSelectedTableIndex(prev => 
          prev > 0 ? prev - 1 : filteredTables.length - 1
        );
        break;
      case 'Enter':
        e.preventDefault();
        if (selectedTableIndex >= 0) {
          selectTable(selectedTableIndex);
        } else {
          handleTableSearch();
        }
        break;
      case 'Escape':
        setShowTableDropdown(false);
        setSelectedTableIndex(-1);
        break;
    }
  };

  // Select script from dropdown
  const selectScript = (index: number) => {
    if (index >= 0 && index < filteredScripts.length) {
      const selectedScript = filteredScripts[index];
      console.log('Selecting script:', selectedScript);
      setScriptSearch(selectedScript);
      setShowScriptDropdown(false);
      setSelectedScriptIndex(-1);
      onScriptChange(selectedScript);
      
      // Auto-apply the search filter
      if (onScriptSearch) {
        onScriptSearch(selectedScript);
      }
      console.log('Script dropdown should be closed now');
    }
  };

  // Select table from dropdown
  const selectTable = (index: number) => {
    if (index >= 0 && index < filteredTables.length) {
      const selectedTable = filteredTables[index];
      console.log('Selecting table:', selectedTable);
      setTableSearch(selectedTable);
      setShowTableDropdown(false);
      setSelectedTableIndex(-1);
      
      // Auto-apply the search filter
      if (onTableSearch) {
        onTableSearch(selectedTable);
      }
      console.log('Table dropdown should be closed now');
    }
  };

  const handleScriptSearch = () => {
    console.log('Script search clicked!', { scriptSearch, availableScripts });
    if (scriptSearch.trim()) {
      // Find exact match first, then partial match
      const exactMatch = availableScripts.find(script => 
        script.toLowerCase() === scriptSearch.toLowerCase()
      );
      const partialMatch = availableScripts.find(script => 
        script.toLowerCase().includes(scriptSearch.toLowerCase())
      );
      
      const match = exactMatch || partialMatch;
      console.log('Script search results:', { exactMatch, partialMatch, match });
      if (match) {
        onScriptChange(match);
        if (onScriptSearch) {
          console.log('Calling onScriptSearch with:', match);
          onScriptSearch(match);
        }
        console.log('Found script:', match);
      } else {
        console.log('No script found matching:', scriptSearch);
        alert(`No script found matching "${scriptSearch}"`);
      }
    } else {
      console.log('Script search is empty, calling onScriptSearch with empty string');
      if (onScriptSearch) {
        onScriptSearch('');
      }
    }
  };

  const handleTableSearch = () => {
    console.log('Table search clicked!', { tableSearch, availableTables });
    if (tableSearch.trim()) {
      // Find exact match first, then partial match
      const exactMatch = availableTables.find(table => 
        table.toLowerCase() === tableSearch.toLowerCase()
      );
      const partialMatch = availableTables.find(table => 
        table.toLowerCase().includes(tableSearch.toLowerCase())
      );
      
      const match = exactMatch || partialMatch;
      console.log('Table search results:', { exactMatch, partialMatch, match });
      if (match) {
        if (onTableSearch) {
          console.log('Calling onTableSearch with:', match);
          onTableSearch(match);
        }
        console.log('Found table:', match);
      } else {
        console.log('No table found matching:', tableSearch);
        alert(`No table found matching "${tableSearch}"`);
      }
    } else {
      console.log('Table search is empty, calling onTableSearch with empty string');
      if (onTableSearch) {
        onTableSearch('');
      }
    }
  };


  const clearScriptFilter = () => {
    // Clear script search input and dropdown
    setScriptSearch('');
    setShowScriptDropdown(false);
    setSelectedScriptIndex(-1);
    
    // Clear script filters
    onScriptChange(null);
    if (onScriptSearch) {
      onScriptSearch('');
    }
  };

  const clearTableFilter = () => {
    // Clear table search input and dropdown
    setTableSearch('');
    setShowTableDropdown(false);
    setSelectedTableIndex(-1);
    
    // Clear table filters
    if (onTableSearch) {
      onTableSearch('');
    }
  };

  const clearAll = () => {
    // Clear search inputs
    setScriptSearch('');
    setTableSearch('');
    setShowScriptDropdown(false);
    setShowTableDropdown(false);
    setSelectedScriptIndex(-1);
    setSelectedTableIndex(-1);
    
    // Clear filters
    onScriptChange(null);
    if (onScriptSearch) {
      onScriptSearch('');
    }
    if (onTableSearch) {
      onTableSearch('');
    }
    
    // Call the clear all callback if provided
    if (onClearAll) {
      onClearAll();
    }
  };

  // Generate filter display text
  const generateFilterText = () => {
    let filterText = 'Filter: ';
    let hasFilters = false;
    
    // Add script filter
    if (selectedScript) {
      filterText += `Scripts: [${selectedScript}]`;
      hasFilters = true;
    }
    
    // Add table filter
    if (selectedTableFilters && selectedTableFilters.length > 0) {
      if (hasFilters) {
        filterText += ', ';
      }
      filterText += `Tables: [${selectedTableFilters.join(', ')}]`;
      hasFilters = true;
    }
    
    // Add connection mode
    if (connectionMode !== 'direct') {
      if (hasFilters) {
        filterText += ', ';
      }
      filterText += `Mode: ${connectionMode}`;
      hasFilters = true;
    }
    
    // Add flow view status
    if (flowViewEnabled) {
      if (hasFilters) {
        filterText += ', ';
      }
      filterText += 'Flow View: ON';
      hasFilters = true;
    }
    
    // Add lock filters status
    if (lockViewEnabled) {
      if (hasFilters) {
        filterText += ', ';
      }
      filterText += 'Filters Locked';
      hasFilters = true;
    }
    
    return hasFilters ? filterText : 'Filter: None';
  };

  return (
    <div className="network-controls">
      <div style={{ fontSize: '1.0em', fontWeight: 'bold', color: '#1976d2', marginBottom: '8px' }}>
        {generateFilterText()}
      </div>
      
      <div style={{ marginBottom: '16px', display: 'flex', gap: '16px', alignItems: 'center' }}>
        <button 
          className="network-insight-btn" 
          onClick={onShowStatistics}
          title="Show network statistics and insights"
        >
          📊 Network Insight
        </button>
        
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <label htmlFor="connectionModeSelect" style={{ fontWeight: 'bold', color: '#495057', whiteSpace: 'nowrap' }}>
            Mode:
          </label>
          <select 
            id="connectionModeSelect" 
            value={connectionMode}
            onChange={(e) => onConnectionModeChange(e.target.value as any)}
            style={{ padding: '4px 8px', border: '1px solid #ccc', borderRadius: '4px', fontSize: '14px', cursor: 'pointer' }}
          >
            <option value="direct">Direct</option>
            <option value="impacts_by">Impacts</option>
            <option value="impacted_by">Impacted By</option>
            <option value="both">Both</option>
          </select>
        </div>
        
        <div className="autocomplete-container" style={{ flex: '1 1 0', display: 'flex', gap: '8px', alignItems: 'center', position: 'relative' }}>
          <div style={{ position: 'relative', width: '100%' }}>
            <input 
              ref={scriptInputRef}
              type="text" 
              placeholder="Search script name..." 
              value={scriptSearch}
              onChange={handleScriptInputChange}
              onKeyDown={handleScriptKeyDown}
              style={{ width: '100%', padding: '8px', border: '1px solid #ccc', borderRadius: '4px', fontSize: '15px' }}
            />
            {showScriptDropdown && (
              <div 
                ref={scriptDropdownRef}
                className="autocomplete-dropdown"
                style={{
                  position: 'absolute',
                  top: '100%',
                  left: 0,
                  right: 0,
                  backgroundColor: '#f0f8ff',
                  border: '2px solid #007bff',
                  borderTop: 'none',
                  borderRadius: '0 0 4px 4px',
                  maxHeight: '200px',
                  overflowY: 'auto',
                  zIndex: 10000,
                  display: 'block',
                  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
                }}
              >
                {filteredScripts.map((script, index) => (
                  <div
                    key={script}
                    className={`autocomplete-item ${index === selectedScriptIndex ? 'selected' : ''}`}
                    onClick={() => selectScript(index)}
                    style={{
                      padding: '8px 12px',
                      cursor: 'pointer',
                      borderBottom: '1px solid #f0f0f0',
                      backgroundColor: index === selectedScriptIndex ? '#007bff' : 'transparent',
                      color: index === selectedScriptIndex ? 'white' : 'inherit'
                    }}
                  >
                    {script}
                  </div>
                ))}
              </div>
            )}
          </div>
          <button 
            onClick={handleScriptSearch}
            style={{
              backgroundColor: '#007bff',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 16px',
              fontWeight: 'bold',
              cursor: 'pointer',
              fontSize: '14px',
              whiteSpace: 'nowrap'
            }}
          >
            Search
          </button>
          <button 
            onClick={clearScriptFilter}
            style={{
              backgroundColor: '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 16px',
              fontWeight: 'bold',
              cursor: 'pointer',
              fontSize: '14px',
              whiteSpace: 'nowrap'
            }}
          >
            Clear
          </button>
        </div>
        
        <div className="autocomplete-container" style={{ flex: '2 1 0', display: 'flex', gap: '8px', alignItems: 'center', position: 'relative' }}>
          <div style={{ position: 'relative', width: '100%' }}>
            <input 
              ref={tableInputRef}
              type="text" 
              placeholder="Search table name..." 
              value={tableSearch}
              onChange={handleTableInputChange}
              onKeyDown={handleTableKeyDown}
              style={{ width: '100%', padding: '8px', border: '1px solid #ccc', borderRadius: '4px', fontSize: '15px' }}
            />
            {showTableDropdown && (
              <div 
                ref={tableDropdownRef}
                className="autocomplete-dropdown"
                style={{
                  position: 'absolute',
                  top: '100%',
                  left: 0,
                  right: 0,
                  backgroundColor: '#f0f8ff',
                  border: '2px solid #007bff',
                  borderTop: 'none',
                  borderRadius: '0 0 4px 4px',
                  maxHeight: '200px',
                  overflowY: 'auto',
                  zIndex: 10000,
                  display: 'block',
                  boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
                }}
              >
                {filteredTables.map((table, index) => (
                  <div
                    key={table}
                    className={`autocomplete-item ${index === selectedTableIndex ? 'selected' : ''}`}
                    onClick={() => selectTable(index)}
                    style={{
                      padding: '8px 12px',
                      cursor: 'pointer',
                      borderBottom: '1px solid #f0f0f0',
                      backgroundColor: index === selectedTableIndex ? '#007bff' : 'transparent',
                      color: index === selectedTableIndex ? 'white' : 'inherit'
                    }}
                  >
                    {table}
                  </div>
                ))}
              </div>
            )}
          </div>
          <button 
            onClick={handleTableSearch}
            style={{
              backgroundColor: '#007bff',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 16px',
              fontWeight: 'bold',
              cursor: 'pointer',
              fontSize: '14px',
              whiteSpace: 'nowrap'
            }}
          >
            Search
          </button>
          <button 
            onClick={clearTableFilter}
            style={{
              backgroundColor: '#6c757d',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              padding: '8px 16px',
              fontWeight: 'bold',
              cursor: 'pointer',
              fontSize: '14px',
              whiteSpace: 'nowrap'
            }}
          >
            Clear
          </button>
        </div>
      </div>
      
      <div style={{ marginBottom: '16px', display: 'flex', gap: '16px', alignItems: 'center' }}>
        <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontWeight: 'bold', color: '#495057', whiteSpace: 'nowrap' }} title="Prevent accidental filter changes when clicking nodes">
          <input 
            type="checkbox" 
            checked={lockViewEnabled}
            onChange={(e) => onLockViewToggle(e.target.checked)}
            style={{ width: '16px', height: '16px', cursor: 'pointer' }}
          />
          Lock Filters
        </label>
        
        <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontWeight: 'bold', color: '#495057', whiteSpace: 'nowrap' }}>
          <input 
            type="checkbox" 
            checked={flowViewEnabled}
            onChange={(e) => onFlowViewToggle(e.target.checked)}
            style={{ width: '16px', height: '16px', cursor: 'pointer' }}
          />
          Flow View
        </label>
      </div>
    </div>
  );
};

export default NetworkControls;

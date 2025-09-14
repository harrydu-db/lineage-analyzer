import React, { useState } from 'react';
import { LineageData } from '../types/LineageData';
import TablesTab from './tabs/TablesTab';
import StatementsTab from './tabs/StatementsTab';
import NetworkTab from './tabs/NetworkTab';

interface TabSectionProps {
  data: LineageData;
  selectedTable: string | null;
  onTableSelect: (tableName: string | null) => void;
}

type TabType = 'tables' | 'statements' | 'network';

const TabSection: React.FC<TabSectionProps> = ({ data, selectedTable, onTableSelect }) => {
  const [activeTab, setActiveTab] = useState<TabType>('tables');
  
  // Network tab state
  const [selectedNetworkScript, setSelectedNetworkScript] = useState<string | null>(null);
  const [selectedTableFilters, setSelectedTableFilters] = useState<string[]>([]);

  const tabs = [
    { id: 'tables', label: '📋 Tables', icon: '📋' },
    { id: 'statements', label: '🔧 Statements', icon: '🔧' },
    { id: 'network', label: '🕸️ Network View', icon: '🕸️' }
  ];

  const renderTabContent = () => {
    switch (activeTab) {
      case 'tables':
        return (
          <TablesTab 
            data={data} 
            selectedTable={selectedTable}
            onTableSelect={onTableSelect}
          />
        );
      case 'statements':
        return (
          <StatementsTab 
            data={data}
          />
        );
      case 'network':
        return (
          <NetworkTab 
            data={data}
            selectedNetworkScript={selectedNetworkScript}
            setSelectedNetworkScript={setSelectedNetworkScript}
            selectedTableFilters={selectedTableFilters}
            setSelectedTableFilters={setSelectedTableFilters}
          />
        );
      default:
        return null;
    }
  };

  return (
    <div className="tab-section">
      <div className="tab-navigation">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`tab-btn ${activeTab === tab.id ? 'active' : ''}`}
            onClick={() => setActiveTab(tab.id as TabType)}
          >
            {tab.label}
          </button>
        ))}
      </div>
      
      <div className="tab-content active">
        {renderTabContent()}
      </div>
    </div>
  );
};

export default TabSection;

import React from 'react';

const LoadingMessage: React.FC = () => {
  return (
    <div className="loading">
      <h3>🚀 Welcome to the Lineage Analyzer</h3>
      <p>Get started by loading a lineage JSON file to explore data relationships and dependencies</p>
      <div style={{ marginTop: '20px', padding: '15px', backgroundColor: '#f8f9fa', borderRadius: '8px', border: '1px solid #e9ecef' }}>
        <h4 style={{ margin: '0 0 10px 0', color: '#495057' }}>How to load data:</h4>
        <p style={{ margin: '0 0 8px 0', color: '#6c757d' }}><strong>Upload a file:</strong> Use the "Load JSON File" button above</p>
        <p style={{ margin: '0', color: '#6c757d' }}><strong>Load folder:</strong> Use the "Load Folder" button to process multiple files</p>
      </div>
    </div>
  );
};

export default LoadingMessage;

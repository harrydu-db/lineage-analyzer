# ETL Lineage Viewer - React Application

This is the React version of the ETL Lineage Viewer, converted from the original vanilla JavaScript implementation with enhanced features and improved functionality.

## Features

- **File Upload**: Load lineage data from individual script JSON files
- **Table View**: Browse and explore table relationships with detailed information
- **Statements View**: View and interact with SQL statements
- **Network View**: Interactive network visualization using vis.js with advanced filtering
- **Connection Modes**: 
  - Direct: Show only directly connected tables
  - Impacts: Show all tables that selected tables can reach (downstream)
  - Impacted By: Show all tables that can reach selected tables (upstream)
  - Both: Show connections in both directions
- **Table Filtering**: Filter network view by specific tables (single table at a time)
- **Script Filtering**: Filter network view by specific scripts
- **Flow View**: Hierarchical layout for better visualization
- **Network Statistics**: Detailed analysis of the lineage network
- **Responsive Design**: Modern UI with resizable panels and fullscreen support

## Development

### Prerequisites

- Node.js (v22.14.0 or later)
- npm

### Running the Development Server

```bash
# Install dependencies
npm install

# Start development server
npm start
```

The application will open at `http://localhost:3000`

### Building for Production

```bash
npm run build
```

This creates a `build` folder with optimized production files.

## Project Structure

```
src/
├── components/
│   ├── tabs/
│   │   ├── TablesTab.tsx          # Table listing and details view
│   │   ├── StatementsTab.tsx      # SQL statements browser
│   │   ├── NetworkTab.tsx         # Interactive network visualization
│   │   └── NetworkControls.tsx    # Network filtering and controls
│   ├── Header.tsx                 # Application header
│   ├── FileInputSection.tsx       # File upload and data loading
│   ├── TabSection.tsx             # Tab navigation and content switching
│   ├── TableDetails.tsx           # Table details modal
│   ├── NetworkStatistics.tsx      # Network analysis and statistics
│   └── SqlModal.tsx               # SQL statement viewer modal
├── types/
│   └── LineageData.ts             # TypeScript type definitions
├── App.tsx                        # Main application component
├── App.css                        # Application styles
└── index.tsx                      # Application entry point
```

## Key Components

- **App.tsx**: Main application component with state management and data conversion
- **TabSection.tsx**: Tab navigation and content switching
- **TablesTab.tsx**: Table listing and details view with relationship exploration
- **StatementsTab.tsx**: SQL statements browser with syntax highlighting
- **NetworkTab.tsx**: Interactive network visualization with advanced filtering
- **NetworkControls.tsx**: Network filtering controls (scripts, tables, connection modes)
- **NetworkStatistics.tsx**: Comprehensive network analysis and statistics
- **FileInputSection.tsx**: File upload and data loading
- **TableDetails.tsx**: Detailed table information modal

## Data Format

The application expects JSON data in the multi-script format. Each script file should contain:

```json
{
  "script_name": "ScriptName.sql",
  "parser_version": "SQLGlot",
  "tables": {
    "table_name": {
      "name": "table_name",
      "is_volatile": false,
      "is_view": false,
      "source": [
        {
          "name": "source_table",
          "operation": [0, 1, 2]
        }
      ],
      "target": [
        {
          "name": "target_table", 
          "operation": [3, 4]
        }
      ]
    }
  },
  "bteq_statements": [
    "SELECT * FROM source_table;",
    "INSERT INTO target_table...",
    "UPDATE table_name SET..."
  ]
}
```

The application automatically loads all `*_sql_lineage.json` files from the `/public/report/` directory and consolidates them into a unified lineage view with proper operation tracking across multiple scripts.

## Integration with Backend

The React application is designed to work with the FastAPI backend:

- Development: `http://localhost:3000` (React dev server)
- Production: `http://localhost:8000/react/` (served by FastAPI)

## Recent Improvements

### Enhanced Network Visualization
- **Consolidated Edges**: Multiple scripts can now contribute operations to the same edge, showing all related operations in a single connection
- **Advanced Filtering**: Filter by scripts, tables, and connection modes with real-time updates
- **Connection Modes**: 
  - Direct: Immediate table connections
  - Impacts: Downstream impact analysis
  - Impacted By: Upstream dependency analysis
  - Both: Complete relationship analysis
- **Single Table Filter**: Focus on one table at a time for cleaner analysis
- **Flow View**: Hierarchical layout for better data flow visualization

### Improved Data Handling
- **Multi-Script Support**: Native support for multiple script files with proper operation consolidation
- **Auto-Loading**: Automatic loading of lineage data from report folders
- **Better Error Handling**: Improved error messages and data validation
- **Performance Optimization**: Efficient data processing and rendering

## Migration from Vanilla JS

This React version maintains feature parity with the original vanilla JavaScript implementation while providing:

- Better code organization and maintainability
- Type safety with TypeScript
- Component reusability
- Modern React patterns and hooks
- Improved state management
- Enhanced network visualization capabilities
- Better user experience with advanced filtering

## Dependencies

### Core Dependencies
- **React 19.1.1**: Modern React with hooks and concurrent features
- **TypeScript 4.9.5**: Type safety and enhanced development experience

### Visualization
- **vis-network 9.1.9**: Interactive network visualization
- **vis-data 7.1.9**: Data management for vis.js

### Testing
- **@testing-library/react 16.3.0**: React component testing
- **@testing-library/jest-dom 6.8.0**: Custom Jest matchers
- **@testing-library/user-event 13.5.0**: User interaction testing

### Development
- **react-scripts**: Create React App build tools
- **@types/jest**: TypeScript definitions for Jest
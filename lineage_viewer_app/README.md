# ETL Lineage Viewer - React Application

A modern React-based web application built with FastAPI backend for visualizing data lineage relationships from ETL scripts. Features a responsive React frontend with TypeScript support and interactive network visualization.

## Project Structure

```
lineage_viewer_app/
├── app.py                          # FastAPI application entry point
├── requirements.txt                 # Python dependencies
├── app.yaml                        # Databricks app manifest
├── README.md                       # This file
├── lineage_viewer_react/           # React application
│   ├── public/                     # Static assets
│   ├── src/                        # React source code
│   │   ├── components/             # React components
│   │   │   ├── tabs/              # Tab-specific components
│   │   │   ├── Header.tsx         # Main header component
│   │   │   ├── TabSection.tsx     # Tab container
│   │   │   └── ...                # Other components
│   │   ├── types/                 # TypeScript type definitions
│   │   ├── App.tsx                # Main React application
│   │   └── index.tsx              # React entry point
│   ├── package.json               # Node.js dependencies
│   ├── tsconfig.json              # TypeScript configuration
│   └── build/                     # Built React application (generated)
└── report/                        # Example lineage report data

```

## Features

### React Frontend
- **⚛️ Modern React Interface**: Built with React 19 and TypeScript for optimal performance
- **📱 Responsive Design**: Works seamlessly on desktop and mobile devices
- **🔍 Advanced Search & Filter**: Real-time search across tables, statements, and scripts
- **📊 Interactive Network Visualization**: Powered by vis.js for smooth network exploration
- **📋 Virtualized Lists**: Efficient handling of large datasets with smooth scrolling
- **🎯 Multi-tab Interface**: Organized tabs for Tables, Statements, and Network views

### Backend Features
- **🚀 FastAPI Backend**: High-performance Python web framework
- **📁 Static File Serving**: Serves React build files and report data
- **🔄 Data Processing**: Converts lineage data to React-compatible format
- **📊 Report Integration**: Automatic loading from report folder
- **🌐 CORS Support**: Proper cross-origin resource sharing configuration


## Local Development

### Prerequisites
- Python 3.10 or higher
- Node.js 16+ and npm
- Modern web browser

### Setup Instructions

1. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Install React dependencies**:
   ```bash
   cd lineage_viewer_react
   npm install
   ```

3. **Build the React application**:
   ```bash
   npm run build
   ```

4. **Run the application**:
   ```bash
   cd ..
   python app.py
   ```

5. **Access the app**:
   - Main page: http://localhost:8000/ (serves the React application)

### Development Mode

For React development with hot reloading:

1. **Terminal 1 - Start React development server**:
   ```bash
   cd lineage_viewer_react
   npm start
   ```
   This will start the React app on http://localhost:3000

2. **Terminal 2 - Start FastAPI server for data**:
   ```bash
   python app.py
   ```
   This serves the API and data on http://localhost:8000

## Databricks App Deployment

### Pre-deployment Steps

1. **Build the React application**:
   ```bash
   cd lineage_viewer_react
   npm run build
   cd ..
   ```

2. **Verify file structure**:
   - Ensure `lineage_viewer_react/build/` directory exists with built files
   - Verify `app.yaml` contains the correct configuration
   - Check that `report/` directory contains your lineage data

### Deployment Process

1. **Deploy to Databricks**:
   - Follow the [Databricks Apps documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
   - Use the Databricks CLI or UI to deploy your app

2. **App Configuration**:
   - **Name**: ETL Lineage Viewer
   - **Entry Point**: app:app
   - **Runtime**: Python 3.9+
   - **Dependencies**: Listed in requirements.txt
   - **Static Files**: React build files served from `/static` and `/assets`

### Post-deployment

- The app will automatically serve the React application from the root path
- Report data will be available at `/report/` endpoint
- All React assets will be served with proper caching headers

## API Endpoints

- `GET /` - Serves the React application (index.html)
- `GET /static/*` - Serves React static assets (JS, CSS, images)
- `GET /assets/*` - Serves additional React build files
- `GET /report/*` - Serves lineage report data files

## Static Files

The app serves the following static files:

### React Application Files
- **Root (`/`)**: Serves the main React application (index.html)
- **Static Assets (`/static/`)**: JavaScript bundles, CSS files, and other static assets
- **Additional Assets (`/assets/`)**: Additional React build files and resources

### Report Data Files (`/report/`)
- Contains lineage report data files (JSON format)
- Automatically loaded by the React application on startup
- Use these files as input examples for the lineage viewer
- Files include:
  - `all_lineage.txt` - List of all available lineage files
  - `*_lineage.json` - Individual lineage report files

## Requirements

### Python Dependencies
- Python 3.9+
- FastAPI 0.104.1
- Uvicorn 0.24.0
- Python-multipart 0.0.6

### React Dependencies
- Node.js 16+
- npm or yarn
- React 19.1.1
- TypeScript 4.9.5
- vis-network 9.1.9 (for network visualization)
- react-router-dom 6.28.0

## Notes

### React Application
- The React app is built using Create React App with TypeScript support
- All React components are modular and reusable
- The app automatically loads data from the `report` directory on startup
- Network visualization is powered by vis.js for smooth performance
- The application is fully responsive and works on mobile devices

### Backend Configuration
- FastAPI serves the React build files from the root path
- Static assets are served with proper caching headers
- The app includes proper CORS handling for development
- Report data is served from the `/report/` endpoint
- The application supports both development and production modes

### Data Processing
- Lineage data is automatically converted to React-compatible format
- The app supports loading multiple script files simultaneously
- Table relationships are processed and displayed in real-time
- SQL statements are formatted and displayed with syntax highlighting

# ETL Lineage Viewer - Databricks App

A Databricks app built with FastAPI that serves a static HTML application for visualizing data lineage relationships from ETL scripts.

## Project Structure

```
lineage_viewer_app/
├── app.py                 # FastAPI application entry point
├── requirements.txt        # Python dependencies
├── app.yaml               # Databricks app manifest
├── README.md              # This file
├── lineage_viewer/        # Static HTML files
│   ├── index.html
│   ├── lineage_viewer.js
│   └── default.css
└── report/                # Example lineage report. Use it as input.

```

## Features

- **Static File Serving**: Serves HTML, CSS, and JavaScript files from the `lineage_viewer` directory
- **Example Data Access**: Serves example report files from the `report` directory
- **Automatic Redirect**: Root path (`/`) automatically redirects to `/lineage_viewer/index.html`


## Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the application**:
   ```bash
   python app.py
   ```

3. **Access the app**:
   - Main page: http://localhost:8000/ (redirects to lineage viewer)
   - Lineage viewer: http://localhost:8000/lineage_viewer/index.html

## Databricks App Deployment

1. **Prepare your app**:
   - Ensure all files are in the correct directory structure
   - Verify `app.yaml` contains the correct configuration

2. **Deploy to Databricks**:
   - Follow the [Databricks Apps documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
   - Use the Databricks CLI or UI to deploy your app

3. **App Configuration**:
   - **Name**: ETL Lineage Viewer
   - **Entry Point**: app:app
   - **Runtime**: Python 3.9
   - **Dependencies**: Listed in requirements.txt

## API Endpoints

- `GET /` - Redirects to `/lineage_viewer/index.html`
- `GET /lineage_viewer/` - Serves files from the lineage_viewer directory
- `GET /report/` - Serves example data files

## Static Files

The app serves the following static files:

### Lineage Viewer Files (`/lineage_viewer/`)
- `index.html` - Main HTML interface
- `lineage_viewer.js` - JavaScript functionality
- `default.css` - Styling

### Example Data Files (`/report/`)
- Contains sample lineage data files for testing and demonstration
- Use these files as input examples for the lineage viewer

## Requirements

- Python 3.9+
- FastAPI 0.104.1
- Uvicorn 0.24.0
- Python-multipart 0.0.6

## Notes

- The app is configured to serve static files from both `lineage_viewer` and `report` directories
- All HTML, CSS, and JavaScript files are served as-is
- The app includes proper CORS handling and static file serving
- Root path automatically redirects to the lineage viewer for better user experience
- The `report` directory contains example data files for testing and demonstration purposes

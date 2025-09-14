"""
ETL Lineage Viewer
A FastAPI application that serves both the legacy HTML viewer and the new React application.
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse

import os
import uvicorn

# Create FastAPI app instance
app = FastAPI(
    title="ETL Lineage Viewer",
    description="Visualize data lineage relationships from your ETL scripts",
    version="2.0.0"
)

# Mount static files directories
app.mount("/report", StaticFiles(directory="lineage_viewer_react/build/report"), name="report")

# Serve React app build files from root
app.mount("/static", StaticFiles(directory="lineage_viewer_react/build/static"), name="static")
app.mount("/assets", StaticFiles(directory="lineage_viewer_react/build"), name="assets")

@app.get("/")
async def read_root():
    """Serve React application from root."""
    return FileResponse("lineage_viewer_react/build/index.html")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

"""
Databricks App - ETL Lineage Viewer
A FastAPI application that serves static HTML files for visualizing data lineage relationships.
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
    version="1.0.0"
)

# Mount static files directories
app.mount("/lineage_viewer", StaticFiles(directory="lineage_viewer"), name="lineage_viewer")
app.mount("/report", StaticFiles(directory="report"), name="report")

@app.get("/")
async def read_root():
    """Redirect root path to lineage viewer."""
    return RedirectResponse(url="/lineage_viewer/index.html")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

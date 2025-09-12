"""
ETL Lineage Analyzer Package - SQLGlot Version

This package provides tools for analyzing ETL shell scripts and SQL files
to extract comprehensive data lineage information using SQLGlot as the SQL parser.
"""

from .lineage import ETLLineageAnalyzerSQLGlot, LineageInfo, TableOperation

__all__ = ['ETLLineageAnalyzerSQLGlot', 'LineageInfo', 'TableOperation']

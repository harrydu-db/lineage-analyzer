# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial open source release
- Comprehensive documentation
- Contributing guidelines
- Code of conduct
- Development setup instructions

## [1.0.0] - 2024-01-XX

### Added
- ETL lineage analysis for shell scripts and SQL files
- Support for `.sh`, `.ksh`, and `.sql` file types
- Batch processing capabilities
- Multiple output formats (JSON, HTML, text)
- Teradata SQL parsing with complex query support
- Table relationship mapping
- Line number tracking for operations
- Volatile table detection
- Comprehensive error handling

### Features
- **Batch Processing**: Process all supported files in a folder automatically
- **Multiple Output Formats**: Generate JSON, HTML, and text reports
- **Robust SQL Parsing**: Handles complex Teradata SQL with subqueries, aliases, and nested operations
- **Comprehensive Analysis**: Extracts source tables, target tables, volatile tables, and operation details
- **Line Number Tracking**: Provides accurate line numbers for each operation
- **Table Relationship Mapping**: Shows data flow between tables

### Supported Operations
- CREATE VOLATILE TABLE
- INSERT INTO
- UPDATE
- SELECT
- JOIN Operations (LEFT OUTER JOIN, RIGHT OUTER JOIN, INNER JOIN)

### Technical Details
- Python 3.7+ compatibility
- Uses sqlparse library for SQL parsing
- Enhanced regex patterns for table extraction
- Intelligent file type detection
- Graceful error handling for malformed files

---

## Version History

- **1.0.0**: Initial release with core ETL lineage analysis functionality 
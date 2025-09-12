#!/usr/bin/env python3
"""
Setup script for ETL Lineage Analyzer
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="etl-lineage-analyzer",
    version="1.0.0",
    author="ETL Lineage Analyzer Contributors",
    author_email="your-email@example.com",
    description="A Python tool for analyzing ETL shell scripts and SQL files to extract data lineage information",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-username/lineage-analyzer",
    project_urls={
        "Bug Tracker": "https://github.com/your-username/lineage-analyzer/issues",
        "Documentation": "https://github.com/your-username/lineage-analyzer#readme",
        "Source Code": "https://github.com/your-username/lineage-analyzer",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Text Processing :: Filters",
        "Topic :: Utilities",
    ],
    python_requires=">=3.10",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
            "pre-commit>=2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "lineage-analyzer=lineage_analyzer.lineage:main",
        ],
    },
    keywords="etl, lineage, sql, data, analysis, teradata, shell-scripts",
    include_package_data=True,
    zip_safe=False,
) 
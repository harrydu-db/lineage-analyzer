#!/usr/bin/env python3
"""
Command-line interface for SQL Extractor

Simple wrapper script to extract SQL statements from shell files.
"""

import sys
import os
from pathlib import Path

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sql_extractor import SQLExtractor


def main():
    """Main function for command-line interface."""
    if len(sys.argv) < 3:
        print("Usage: python extract_sql.py <input_path> <output_folder> [--verbose]")
        print("\nArguments:")
        print("  input_path     Path to a shell file or folder containing .sh files")
        print("  output_folder  Path to the output folder for extracted SQL files")
        print("  --verbose      Enable verbose logging (optional)")
        print("\nExamples:")
        print("  python extract_sql.py input/lotmaster/LOT_PROCESS_TRACKING.sh output/extracted_sql --verbose")
        print("  python extract_sql.py input/lotmaster/ output/extracted_sql --verbose")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_folder = sys.argv[2]
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    
    if verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    input_path_obj = Path(input_path)
    
    if input_path_obj.is_file():
        # Process single file
        success = process_single_file(input_path_obj, output_folder)
        if success:
            print(f"\n✅ SQL extraction completed successfully!")
            print(f"📁 Check output folder: {output_folder}")
        else:
            print("\n❌ SQL extraction failed. Check logs for details.")
            sys.exit(1)
    elif input_path_obj.is_dir():
        # Process all .sh files in directory
        success = process_directory(input_path_obj, output_folder)
        if success:
            print(f"\n✅ SQL extraction completed successfully!")
            print(f"📁 Check output folder: {output_folder}")
        else:
            print("\n❌ SQL extraction failed. Check logs for details.")
            sys.exit(1)
    else:
        print(f"❌ Input path does not exist: {input_path}")
        sys.exit(1)


def process_single_file(input_file: Path, output_folder: str) -> bool:
    """Process a single shell file."""
    extractor = SQLExtractor(str(input_file), output_folder)
    return extractor.extract()


def process_directory(input_dir: Path, output_folder: str) -> bool:
    """Process all .sh files in a directory."""
    # Find all .sh files in the directory
    sh_files = list(input_dir.glob("*.sh"))
    
    if not sh_files:
        print(f"❌ No .sh files found in directory: {input_dir}")
        return False
    
    print(f"📁 Found {len(sh_files)} .sh files to process:")
    for file_path in sorted(sh_files):
        print(f"   - {file_path.name}")
    
    print(f"\n🔄 Processing files...")
    
    success_count = 0
    failed_files = []
    
    for file_path in sorted(sh_files):
        print(f"\n📄 Processing: {file_path.name}")
        extractor = SQLExtractor(str(file_path), output_folder)
        if extractor.extract():
            success_count += 1
        else:
            failed_files.append(file_path.name)
    
    # Summary
    print(f"\n📊 Processing Summary:")
    print(f"   ✅ Successfully processed: {success_count}/{len(sh_files)} files")
    if failed_files:
        print(f"   ❌ Failed files: {', '.join(failed_files)}")
        return False
    
    # List generated files
    output_path = Path(output_folder)
    if output_path.exists():
        sql_files = list(output_path.glob("*.sql"))
        if sql_files:
            print(f"\n📄 Generated {len(sql_files)} SQL files:")
            for file_path in sorted(sql_files):
                print(f"   - {file_path.name}")
    
    return True


if __name__ == "__main__":
    main() 
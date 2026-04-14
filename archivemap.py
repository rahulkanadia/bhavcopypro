import os
import shutil
import tempfile
import json
import gzip

def process_file(filepath):
    """Extracts samples or full text based on the file type."""
    filename = os.path.basename(filepath).lower()
    
    # Ignore PDF and DOC/DOCX files
    if filename.endswith(('.pdf', '.doc', '.docx')):
        return None
        
    # Capture readme.txt in full
    if filename == 'readme.txt':
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                return f.read().strip()
        except Exception as e:
            return f"Error reading full file: {e}"
            
    # Capture header + 5 rows for target data files
    target_extensions = ('.csv', '.dat', '.txt')
    if filename.endswith(target_extensions) or filename.endswith('.csv.gz'):
        lines = []
        try:
            # Handle gzipped CSVs
            if filename.endswith('.csv.gz'):
                open_func = lambda p: gzip.open(p, 'rt', encoding='utf-8', errors='replace')
            else:
                open_func = lambda p: open(p, 'r', encoding='utf-8', errors='replace')
                
            with open_func(filepath) as f:
                for _ in range(3):  # Header + 5 sample rows
                    try:
                        lines.append(next(f).strip())
                    except StopIteration:
                        break
            return "\n".join(lines)
        except Exception as e:
            return f"Error reading sample: {e}"
            
    return None

def scan_archive(archive_path, output_json="archive_scan_results.json"):
    """Extracts the archive, handles nested zips, and builds a file map."""
    print(f"Processing archive: {archive_path}")
    results = {}
    
    # Use a temporary directory so we don't clutter your workspace
    with tempfile.TemporaryDirectory() as temp_dir:
        print("Extracting main archive...")
        try:
            shutil.unpack_archive(archive_path, temp_dir)
        except Exception as e:
            print(f"Failed to unpack main archive: {e}")
            return
            
        # First pass: look for nested archives and unpack them in place
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.lower().endswith('.zip'):
                    nested_zip_path = os.path.join(root, file)
                    extract_to = os.path.join(root, file + "_extracted")
                    try:
                        shutil.unpack_archive(nested_zip_path, extract_to)
                    except Exception as e:
                        print(f"Could not unpack nested archive {file}: {e}")
                        
        # Second pass: scan all files (including those from nested archives)
        print("Scanning files and extracting samples...")
        for root, _, files in os.walk(temp_dir):
            for file in files:
                # Skip the raw nested zip files themselves since we extracted them
                if file.lower().endswith('.zip'):
                    continue
                    
                filepath = os.path.join(root, file)
                content_sample = process_file(filepath)
                
                if content_sample is not None:
                    # Create a clean relative path for the output
                    rel_path = os.path.relpath(filepath, temp_dir)
                    results[rel_path] = content_sample
                    
    # Save to JSON for easy copy-pasting back into our chat
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
        
    print(f"Done. Found {len(results)} relevant files.")
    print(f"Results saved to {output_json}")

if __name__ == "__main__":
    # Update this with the path to your main archive
    ARCHIVE_PATH = r"F:\projects\databackfill\BhavCopy samples\Reports-Archives-Multiple-03012020.zip"
    scan_archive(ARCHIVE_PATH)
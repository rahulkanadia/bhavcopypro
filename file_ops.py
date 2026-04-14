import os
import zipfile
import shutil
import re
import tempfile

def sanitize_name(filename):
    """Case-insensitive regex mapping for cryptic exchange names."""
    base_name, ext = os.path.splitext(filename)
    base_lower = base_name.lower()

    # 1. Dynamic Regex Catches (e.g. cm03JAN2020bhav -> CM_Bhavcopy)
    if re.match(r'^cm\d*[a-z]{3}\d*bhav$', base_lower): return f"CM_Bhavcopy{ext}"
    if re.match(r'^fo\d*[a-z]{3}\d*bhav$', base_lower): return f"FO_Bhavcopy{ext}"
    if "bhavcopy_nse_cm" in base_lower: return f"CM_UDiFF_Bhavcopy{ext}"
    if "bhavcopy_nse_fo" in base_lower: return f"FO_UDiFF_Bhavcopy{ext}"
    if "bhavcopy_bse_cm" in base_lower: return f"BSE_CM_UDiFF_Bhavcopy{ext}"
    if "bhavcopy_bse_fo" in base_lower: return f"BSE_FO_UDiFF_Bhavcopy{ext}"
    if base_lower.startswith("eq") and base_lower.endswith("_csv"): return f"BSE_CM_Bhavcopy{ext}"

    # 2. PR Archive & Standard File Mapping
    pr_map = {
        'an': 'Announcements', 'bc': 'Corporate_Actions', 'bh': 'Circuit_Band_Hits',
        'bm': 'Board_Meetings', 'gl': 'Gainers_Losers', 'hl': '52W_High_Low',
        'tt': 'Top_25_Traded', 'pd': 'Pd', 'pr': 'Pr', 'sme': 'SME_Bhavcopy',
        'etf': 'ETF_Bhavcopy', 'corpbond': 'Corporate_Bonds', 'mto': 'Delivery_Positions',
        'shortselling': 'Short_Selling', 'c_var1': 'VaR_Margin_Parameters',
        'cm_52_wk_high_low': '52W_High_Low'
    }

    # Strip numbers to map purely by root identifier
    cleaned = re.sub(r'\d+', '', base_lower).strip('_-')
    if cleaned in pr_map:
        return f"{pr_map[cleaned]}{ext}"

    return filename

def process_downloaded_file(file_path, target_dir, date_obj, exchange, segment, unzip):
    """Extracts, sanitizes, and moves files while preserving the original archive."""
    date_prefix = date_obj.strftime("%Y%m%d")
    prefix = f"{date_prefix}_{exchange}_{segment}_"
    extracted_files = []

    # Check if it's a zip file and unzipping is enabled
    if unzip and file_path.lower().endswith('.zip'):
        # Use a temporary directory to avoid clutter, retain original zip in the target folder
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
            except zipfile.BadZipFile:
                # If the exchange gave us a corrupted zip file, return empty list
                return []

            # Pass 1: Handle nested zips (e.g., cm03JAN2020bhav.csv.zip inside a PR archive)
            # Use a loop to ensure we catch newly extracted nested zips
            nested_zips_found = True
            while nested_zips_found:
                nested_zips_found = False
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        if file.lower().endswith('.zip'):
                            nested_zip = os.path.join(root, file)
                            try:
                                with zipfile.ZipFile(nested_zip, 'r') as nz:
                                    nz.extractall(root)
                            except zipfile.BadZipFile:
                                pass # Ignore bad nested zips if they occur
                            
                            os.remove(nested_zip) # Remove the nested zip, NOT the parent archive
                            nested_zips_found = True
                            break # Break and restart os.walk to catch contents just extracted
                    if nested_zips_found:
                        break

            # Pass 2: Move all resulting files to the final target directory
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    old_path = os.path.join(root, file)
                    new_name = f"{prefix}{sanitize_name(file)}"
                    new_path = os.path.join(target_dir, new_name)
                    
                    # Handle overrides quietly
                    if os.path.exists(new_path):
                        os.remove(new_path)
                        
                    shutil.move(old_path, new_path)
                    extracted_files.append(new_path)
                    
        return extracted_files
        
    else:
        # It's a raw file (like .DAT or .csv) or unzip is false
        new_name = f"{prefix}{sanitize_name(os.path.basename(file_path))}"
        new_path = os.path.join(target_dir, new_name)

        # Handle Windows 'FileExistsError' quietly on overrides
        if os.path.exists(new_path) and new_path != file_path:
            os.remove(new_path)
            
        if new_path != file_path:
            # We rename/move the raw file to its sanitized name
            os.rename(file_path, new_path)
            
        return [new_path]
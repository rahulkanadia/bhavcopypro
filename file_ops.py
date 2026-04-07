import os
import zipfile
import shutil
import re

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
    date_prefix = date_obj.strftime("%Y%m%d")
    prefix = f"{date_prefix}_{exchange}_{segment}_"
    
    # Check if it's a zip file (case insensitive for .ZIP)
    if unzip and file_path.lower().endswith('.zip'):
        extract_dir = os.path.join(target_dir, f"temp_{date_prefix}")
        os.makedirs(extract_dir, exist_ok=True)
        
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
            
        os.remove(file_path) # Delete original zip
        
        for root, _, files in os.walk(extract_dir):
            for file in files:
                # If a nested zip exists (e.g. cm03JAN2020bhav.csv.zip inside PR.zip), extract it too
                if file.lower().endswith('.zip'):
                    nested_zip = os.path.join(root, file)
                    with zipfile.ZipFile(nested_zip, 'r') as nz:
                        nz.extractall(root)
                    os.remove(nested_zip)
                    continue

                old_path = os.path.join(root, file)
                new_name = f"{prefix}{sanitize_name(file)}"
                new_path = os.path.join(target_dir, new_name)
                shutil.move(old_path, new_path)
                
        shutil.rmtree(extract_dir)
        return os.path.join(target_dir, f"{prefix}Extracted_Archive")
    else:
        # It's a raw file or unzip is false
        new_name = f"{prefix}{sanitize_name(os.path.basename(file_path))}"
        new_path = os.path.join(target_dir, new_name)
        
        # Handle Windows 'FileExistsError' quietly on overrides
        if os.path.exists(new_path):
            os.remove(new_path)
            
        os.rename(file_path, new_path)
        return new_path
import os
import zipfile
import shutil
import pandas as pd
import tempfile
import re

def recursive_unzip(target_dir):
    """Recursively unpacks all zips in the workspace."""
    nested_zips_found = True
    while nested_zips_found:
        nested_zips_found = False
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.lower().endswith('.zip'):
                    zip_path = os.path.join(root, file)
                    extract_path = os.path.join(root, file[:-4])
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zf:
                            zf.extractall(extract_path)
                    except Exception: pass
                    try: os.remove(zip_path)
                    except OSError: pass 
                    nested_zips_found = True
                    break 
            if nested_zips_found: break

def clean_columns(df):
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df

def build_spot_master():
    folder = input("Enter path to your main archives folder: ").strip()
    if not os.path.exists(folder):
        print("Folder not found.")
        return

    workspace = os.path.join(folder, "_Unzipped_Spot_Workspace")
    if not os.path.exists(workspace):
        os.makedirs(workspace)
        for root, _, files in os.walk(folder):
            for file in files:
                if file.lower().endswith('.zip') and "_Unzipped" not in root:
                    shutil.copy(os.path.join(root, file), os.path.join(workspace, file))
        print("Unzipping archives...")
        recursive_unzip(workspace)

    print("Mining Spot UIDs...")
    master_dict = {}
    known_etfs = set()

    for root, _, files in os.walk(workspace):
        for file in files:
            f_lower = file.lower()
            full_path = os.path.join(root, file)
            
            # Target only ETF and Cash Bhavcopies
            is_etf = f_lower.startswith('etf') and f_lower.endswith('.csv')
            is_legacy_cash = 'sec_bhavdata_full' in f_lower
            is_udiff_cash = 'bhavcopy_nse_cm' in f_lower
            
            if not (is_etf or is_legacy_cash or is_udiff_cash):
                continue

            try:
                try: df = pd.read_csv(full_path, low_memory=False, on_bad_lines='skip', keep_default_na=False)
                except UnicodeDecodeError: df = pd.read_csv(full_path, low_memory=False, on_bad_lines='skip', encoding='latin1', keep_default_na=False)
                
                df = clean_columns(df)
                
                if is_etf:
                    if 'SYMBOL' in df.columns:
                        known_etfs.update(df['SYMBOL'].astype(str).str.strip().str.upper().tolist())
                    continue

                records = df.to_dict('records')
                for row in records:
                    major, group = "", ""
                    
                    if is_legacy_cash:
                        ticker = str(row.get('SYMBOL', '')).strip().upper()
                        series = str(row.get('SERIES', '')).strip().upper()
                        name = "" # Legacy doesn't always have names in price file
                    else:
                        ticker = str(row.get('TCKRSYMB', '')).strip().upper()
                        series = str(row.get('SCTYSRS', '')).strip().upper()
                        name = str(row.get('FININSTRMNM', '')).strip()

                    if not ticker or ticker == 'NAN': continue

                    # Spot Classification Matrix
                    if series in ['EQ', 'BE', 'BZ']: major, group = "EQUITY", "RIGHTS" if ticker.endswith('-RE') else f"EQUITY {series}"
                    elif series in ['SM', 'ST', 'SZ']: major, group = "EQUITY", "EQUITY SME"
                    elif series in ['E@', 'X@'] or re.match(r'^[EX]\d$', series): major, group = "EQUITY", "RIGHTS PP"
                    elif series in ['MF', 'ME']: major, group = "MUTUAL FUND", "MF"
                    elif re.match(r'^[PQOF][A-Z0-9]$', series): major, group = "EQUITY", "PREFERENCE"
                    elif re.match(r'^[DS][A-Z0-9]$', series): major, group = "DEBT", "CONVERTIBLE DEBT"
                    elif series == 'GS': major, group = "DEBT", "GOVT SEC"
                    elif series == 'GB': major, group = "DEBT", "GOVT BOND"
                    elif series == 'SG': major, group = "DEBT", "STATE BOND"
                    elif series == 'TB': major, group = "DEBT", "TBILL"
                    elif series in ['RR', 'RT']: major, group = "TRUST", "REIT"
                    elif series in ['IV', 'ID']: major, group = "TRUST", "INVIT"
                    elif series.startswith('W'): major, group = "EQUITY", "WARRANT"
                    else: major, group = "DEBT", "CORP BOND" # Catch-all for 2-char Debt series (including "NA")

                    uid = ticker
                    
                    if uid not in master_dict:
                        master_dict[uid] = {
                            'Exchange': 'NSE', 'Major Asset': major, 'Asset Group': group, 
                            'Minor Asset': 'SPOT', 'Name': name, 'Ticker': ticker,
                            'Strike': '', 'Contract Expiry': '', 'Contract Type': '', 'UID': uid
                        }
                    else:
                        # Update name if we found a better one in UDiFF
                        if not master_dict[uid]['Name'] and name:
                            master_dict[uid]['Name'] = name
                            
            except Exception as e:
                print(f"Error parsing {file}: {e}")

    # Retroactive ETF Tagging
    for uid, data in master_dict.items():
        if data['Minor Asset'] == 'SPOT' and data['Ticker'] in known_etfs:
            master_dict[uid]['Major Asset'] = 'MUTUAL FUND'
            master_dict[uid]['Asset Group'] = 'ETF'

    if master_dict:
        df_master = pd.DataFrame.from_dict(master_dict, orient='index')
        df_master.sort_values(by=['Major Asset', 'Asset Group', 'Ticker'], inplace=True)
        out_path = os.path.join(folder, "Spot_Asset_Master.csv")
        df_master.to_csv(out_path, index=False)
        print(f"\n:white_check_mark: Success! Saved {len(df_master)} unique assets to {out_path}")
    else:
        print("\nNo valid data found.")

if __name__ == "__main__":
    build_spot_master()
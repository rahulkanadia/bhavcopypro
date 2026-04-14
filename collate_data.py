import os
import zipfile
import tempfile
import pandas as pd
import concurrent.futures
from datetime import datetime
import threading
import re
import sys
import signal

# Thread locks and shutdown event
csv_write_lock = threading.Lock()
print_lock = threading.Lock()
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    with print_lock:
        print("\n[!] Ctrl+C DETECTED. Terminating all processes immediately...")
    shutdown_event.set()
    sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)

def safe_print(message):
    with print_lock:
        print(message)

def get_clean_columns(df):
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def process_bundle(date_str, file_paths, output_dir):
    if shutdown_event.is_set():
        return

    year = date_str[:4]
    parsed_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    
    df_price, df_volt, df_pe = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # 1. Extract files
        for fpath in file_paths:
            if shutdown_event.is_set(): return
            if fpath.lower().endswith('.zip'):
                try:
                    with zipfile.ZipFile(fpath, 'r') as zf:
                        zf.extractall(temp_dir)
                except Exception:
                    pass
            else:
                import shutil
                shutil.copy(fpath, temp_dir)

        # 2. Extract nested zips
        nested_zips_found = True
        while nested_zips_found:
            if shutdown_event.is_set(): return
            nested_zips_found = False
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file.lower().endswith('.zip'):
                        nested_zip = os.path.join(root, file)
                        try:
                            with zipfile.ZipFile(nested_zip, 'r') as nz:
                                nz.extractall(root)
                        except Exception:
                            pass
                        os.remove(nested_zip)
                        nested_zips_found = True
                        break 
                if nested_zips_found:
                    break

        # 3. Parse Data
        for root, _, files in os.walk(temp_dir):
            if shutdown_event.is_set(): return
            for file in files:
                f_lower = file.lower()
                
                # Strict file targeting
                is_target = False
                if "bhav" in f_lower: is_target = True 
                elif "cmvolt" in f_lower: is_target = True
                elif "pe_" in f_lower and f_lower.endswith('.csv'): is_target = True

                if not is_target:
                    continue

                full_path = os.path.join(root, file)
                
                try:
                    try:
                        df = pd.read_csv(full_path, low_memory=False, on_bad_lines='skip')
                    except UnicodeDecodeError:
                        df = pd.read_csv(full_path, low_memory=False, on_bad_lines='skip', encoding='latin1')
                        
                    df = get_clean_columns(df)
                    cols = df.columns.tolist()
                    
                    # A. UDiFF Schema (Post July 2024)
                    if 'tckrsymb' in cols and 'clspric' in cols:
                        # FIX: Strip whitespace to catch "EQ "
                        eq_df = df[df['sctysrs'].astype(str).str.strip().str.upper() == 'EQ'].copy()
                        if not eq_df.empty and df_price.empty:
                            eq_df['VWAP'] = eq_df['ttltrfval'] / eq_df['ttltradgvol']
                            df_price = eq_df.rename(columns={
                                'tckrsymb': 'Ticker', 'sctysrs': 'Category', 
                                'fininstrmnm': 'Name', 'opnpric': 'Open', 
                                'hghpric': 'High', 'lwpric': 'Low', 
                                'clspric': 'Close', 'ttltradgvol': 'Volume'
                            }, errors='ignore')
                            if 'Delivery %' not in df_price.columns:
                                df_price['Delivery %'] = None

                    # B. Legacy Bhavcopy (Full or Standard)
                    elif 'symbol' in cols and ('close' in cols or 'close_price' in cols):
                        if 'series' not in cols:
                            continue # Ignore F&O bhavcopies which have symbol/close but no series
                            
                        # FIX: Strip whitespace to catch "EQ "
                        eq_df = df[df['series'].astype(str).str.strip().str.upper() == 'EQ'].copy()
                        
                        if not eq_df.empty:
                            rename_map = {
                                'symbol': 'Ticker', 'series': 'Category',
                                'open': 'Open', 'open_price': 'Open',
                                'high': 'High', 'high_price': 'High',
                                'low': 'Low', 'low_price': 'Low',
                                'close': 'Close', 'close_price': 'Close',
                                'avg_price': 'VWAP', 'last': 'LTP', 'last_price': 'LTP',
                                'tottrdqty': 'Volume', 'ttl_trd_qnty': 'Volume',
                                'deliv_per': 'Delivery %'
                            }
                            temp_price = eq_df.rename(columns=rename_map, errors='ignore')
                            
                            if 'VWAP' not in temp_price.columns and 'tottrdval' in df.columns and 'Volume' in temp_price.columns:
                                temp_price['VWAP'] = df['tottrdval'] / temp_price['Volume']
                                
                            if 'Delivery %' not in temp_price.columns:
                                temp_price['Delivery %'] = None

                            # FIX: Only assign if df_price is empty, OR if this new file has Delivery Data and the old one didn't.
                            if df_price.empty or (df_price['Delivery %'].isnull().all() and not temp_price['Delivery %'].isnull().all()):
                                df_price = temp_price

                    # C. Volatility
                    elif 'symbol' in cols and any("volatility" in c for c in cols):
                        if df_volt.empty:
                            df_volt = df.rename(columns={'symbol': 'Ticker'})
                            for col in df_volt.columns:
                                if "current day" in col or "daily volatility" in col:
                                    df_volt.rename(columns={col: 'Current Day Volatility'}, inplace=True)
                                elif "annualised" in col:
                                    df_volt.rename(columns={col: 'Annualized Volatility'}, inplace=True)

                    # D. PE Ratio
                    elif 'symbol' in cols and 'adjusted p/e' in cols:
                        if df_pe.empty:
                            df_pe = df.rename(columns={'symbol': 'Ticker', 'adjusted p/e': 'Adjusted PE'})

                except Exception as e:
                    safe_print(f" [ERROR] Parsing {file} on {parsed_date}: {str(e)}")
                    continue

        # 4. Join and Export
        if df_price.empty:
            safe_print(f" [WARNING] No equity price data found for {parsed_date}.")
            return

        df_final = df_price
        if 'Name' not in df_final.columns:
            df_final['Name'] = ""
            
        if not df_volt.empty and 'Ticker' in df_volt.columns:
            df_final = pd.merge(df_final, df_volt[['Ticker', 'Current Day Volatility', 'Annualized Volatility']], on='Ticker', how='left')
        else:
            df_final['Current Day Volatility'] = None
            df_final['Annualized Volatility'] = None
            
        if not df_pe.empty and 'Ticker' in df_pe.columns:
            df_final = pd.merge(df_final, df_pe[['Ticker', 'Adjusted PE']], on='Ticker', how='left')
        else:
            df_final['Adjusted PE'] = None

        df_final.insert(0, 'Date', parsed_date)
        
        target_cols = ['Date', 'Ticker', 'Category', 'Name', 'Open', 'High', 'Low', 'Close', 'VWAP', 'Volume', 'Delivery %', 'Current Day Volatility', 'Annualized Volatility', 'Adjusted PE']
        existing_cols = [c for c in target_cols if c in df_final.columns]
        df_final = df_final[existing_cols]

        output_file = os.path.join(output_dir, f"{year}_Equity_Bhavcopy.csv")
        
        with csv_write_lock:
            write_header = not os.path.exists(output_file)
            df_final.to_csv(output_file, mode='a', header=write_header, index=False)

        safe_print(f" [+] SUCCESS: {parsed_date} -> Appended {len(df_final)} equities to {year}_Equity_Bhavcopy.csv")

def main():
    root_dir = input("Enter the path to your main archive folder (e.g., F:\\projects\\bhavcopypro\\NSE): ").strip()
    output_dir = input("Enter output folder for yearly CSVs: ").strip()
    os.makedirs(output_dir, exist_ok=True)

    print("Scanning directories for files...")
    daily_bundles = {}

    date_pattern = re.compile(r'(\d{2})(\d{2})(\d{4})')

    for root, _, files in os.walk(root_dir):
        for file in files:
            match = date_pattern.search(file)
            if match:
                dd, mm, yyyy = match.groups()
                date_prefix = f"{yyyy}{mm}{dd}" 
                
                if date_prefix not in daily_bundles:
                    daily_bundles[date_prefix] = []
                daily_bundles[date_prefix].append(os.path.join(root, file))

    if not daily_bundles:
        print("Found 0 unique trading days to process. Check if archives are downloaded and path is correct.")
        return

    print(f"Found {len(daily_bundles)} unique trading days to process. Beginning extraction... (Press Ctrl+C to abort)")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for date_str, file_paths in daily_bundles.items():
            if not shutdown_event.is_set():
                futures.append(executor.submit(process_bundle, date_str, file_paths, output_dir))
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result() 
            except Exception as e:
                if not shutdown_event.is_set():
                    safe_print(f" [CRITICAL] Thread crashed: {e}")

    if not shutdown_event.is_set():
        print("\n--- All years collated successfully! Temp files purged. ---")

if __name__ == "__main__":
    main()
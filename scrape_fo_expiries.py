import os
import time
import pandas as pd
from playwright.sync_api import sync_playwright

# --- Stealth Injection ---
STEALTH_INIT_SCRIPT = """
(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    window.chrome = { runtime: {} };
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
})();
"""

def get_completed_symbols(out_file):
    completed = set()
    if os.path.exists(out_file):
        try:
            df = pd.read_csv(out_file, on_bad_lines='skip', keep_default_na=False)
            for _, row in df.iterrows():
                inst = str(row.get('Instrument', '')).strip()
                sym = str(row.get('Symbol', '')).strip()
                if inst and sym:
                    completed.add(f"{inst}_{sym}")
        except Exception as e:
            print(f"[!] Error reading existing CSV. Starting fresh. ({e})")
    return completed

def run_scraper():
    out_file = "NSE_FO_Expiries.csv"
    completed_symbols = get_completed_symbols(out_file)
    
    if completed_symbols:
        print(f"[+] Found existing save file. Skipping {len(completed_symbols)} already processed Instrument-Symbol combos.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        context.add_init_script(STEALTH_INIT_SCRIPT)
        page = context.new_page()
        
        print("Navigating to NSE F&O Report page...")
        page.goto("https://www.nseindia.com", timeout=60000)
        time.sleep(2)
        
        page.goto("https://www.nseindia.com/report-detail/fo_eq_security", timeout=60000)
        page.wait_for_selector('#hcpFO_instrument', state='attached')
        time.sleep(2)
        
        instruments = page.locator('#hcpFO_instrument option').all_inner_texts()
        valid_instruments = [i.strip() for i in instruments if i.strip() not in ["Select", "Volatility Futures"]]
        
        print(f"Found Instruments: {valid_instruments}")
        
        for inst in valid_instruments:
            # WIPE downstream dropdowns to prevent stale reads
            page.evaluate("document.getElementById('hcpFO_symbol').innerHTML = '<option value=\"\">Select</option>';")
            page.evaluate("document.getElementById('hcpFO_year').innerHTML = '<option value=\"\">Select</option>';")
            page.evaluate("document.getElementById('hcpFO_expiryDt').innerHTML = '<option value=\"\">Select</option>';")
            
            inst_val = page.locator(f'#hcpFO_instrument option:has-text("{inst}")').get_attribute('value')
            page.locator('#hcpFO_instrument').select_option(value=inst_val)
            page.evaluate("document.getElementById('hcpFO_instrument').dispatchEvent(new Event('change'))")
            
            try:
                page.wait_for_function("document.querySelector('#hcpFO_symbol').options.length > 1", timeout=15000)
            except Exception:
                continue
                
            symbols = [s.strip() for s in page.locator('#hcpFO_symbol option').all_inner_texts() if s.strip() != "Select"]
            
            for sym in symbols:
                combo_key = f"{inst}_{sym}"
                if combo_key in completed_symbols:
                    print(f"  -> Skipping {inst} | {sym} (Already saved)")
                    continue

                print(f"Extracting {inst} -> {sym}...")
                
                # WIPE downstream dropdowns
                page.evaluate("document.getElementById('hcpFO_year').innerHTML = '<option value=\"\">Select</option>';")
                page.evaluate("document.getElementById('hcpFO_expiryDt').innerHTML = '<option value=\"\">Select</option>';")
                
                page.locator('#hcpFO_symbol').select_option(label=sym)
                page.evaluate("document.getElementById('hcpFO_symbol').dispatchEvent(new Event('change'))")
                
                try:
                    page.wait_for_function("document.querySelector('#hcpFO_year').options.length > 1", timeout=15000)
                except Exception:
                    continue
                    
                years = [y.strip() for y in page.locator('#hcpFO_year option').all_inner_texts() if y.strip() != "Select"]
                
                symbol_data = [] 
                
                for year in years:
                    # THE FIX: Wipe the expiry dropdown empty BEFORE selecting the year
                    page.evaluate("document.getElementById('hcpFO_expiryDt').innerHTML = '<option value=\"\">Select</option>';")
                    
                    page.locator('#hcpFO_year').select_option(label=year)
                    page.evaluate("document.getElementById('hcpFO_year').dispatchEvent(new Event('change'))")
                    
                    try:
                        # Wait until the FRESH dates load in
                        page.wait_for_function("document.querySelector('#hcpFO_expiryDt').options.length > 1", timeout=15000)
                    except Exception:
                        continue
                        
                    expiries = [e.strip() for e in page.locator('#hcpFO_expiryDt option').all_inner_texts() if e.strip() != "Select"]
                    
                    for expiry in expiries:
                        symbol_data.append({
                            "Instrument": inst,
                            "Symbol": sym,
                            "Year": year,
                            "Expiry": expiry
                        })
                
                if symbol_data:
                    df_chunk = pd.DataFrame(symbol_data)
                    df_chunk.to_csv(out_file, mode='a', header=not os.path.exists(out_file), index=False)
                    completed_symbols.add(combo_key)

        browser.close()
        print("\n:white_check_mark: Extraction Routine Complete!")

if __name__ == "__main__":
    run_scraper()
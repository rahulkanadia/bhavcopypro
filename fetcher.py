import os
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError

class MarketFetcher:
    def __init__(self):
        pass

    def fetch_archive(self, target_date, root_dir):
        d_obj = datetime.strptime(target_date, "%Y-%m-%d")
        
        # NSE strictly requires DD-MMM-YYYY format
        d_str = d_obj.strftime("%d-%b-%Y") 

        exchange = "NSE"
        target_subfolder = os.path.join(root_dir, exchange, str(d_obj.year), f"{d_obj.month:02d}")
        os.makedirs(target_subfolder, exist_ok=True)

        with sync_playwright() as p:
            # 1. BOT EVASION: Disable blink features that flag automation
            browser = p.chromium.launch(
                headless=False, 
                args=["--disable-blink-features=AutomationControlled"]
            )
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                accept_downloads=True,
                viewport={"width": 1366, "height": 768}
            )

            # 2. BOT EVASION: Override the navigator.webdriver property before the page loads
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            page = context.new_page()

            try:
                page.goto("https://www.nseindia.com/all-reports", timeout=60000, wait_until="domcontentloaded")
                time.sleep(2) 

                page.locator("a#Archives_rpt").click()
                time.sleep(1)

                page.evaluate(f'''
                    var dateInput = document.getElementById('cr_equity_archives_date');
                    if (dateInput) {{
                        dateInput.removeAttribute('readonly');
                        dateInput.value = '{d_str}';
                        dateInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    }}
                ''')

                try:
                    page.wait_for_selector('#cr_equity_archives .reportsDownload', timeout=15000)
                except TimeoutError:
                    browser.close()
                    raise Exception("No data loaded. Confirmed market holiday or weekend.")

                time.sleep(2)

                # Natively click Select All
                select_all = page.locator('#cr_equity_archives label.chk_container:has-text("Select All Reports")')
                select_all.click()
                
                # Give the frontend JS time to process the selection
                time.sleep(3) 

                # 3. DOM SAFETY NET: Force sync the checkbox state in case the label click was too fast for the JS framework
                page.evaluate('''
                    document.querySelectorAll('#cr_equity_archives .reportsDownload input[type="checkbox"]').forEach(chk => {
                        if(!chk.checked) { 
                            chk.checked = true; 
                            chk.dispatchEvent(new Event('change', { bubbles: true })); 
                        }
                    });
                ''')
                time.sleep(2)

                # 4. HUMAN MIMICRY: Hover over the button first, pause, then click
                with page.expect_download(timeout=90000) as download_info:
                    download_btn = page.locator('#cr_equity_archives a.link.ms-3:has-text("Multiple file Download")')
                    download_btn.hover()
                    time.sleep(1)
                    download_btn.click()

                download = download_info.value
                filename = download.suggested_filename
                dest_path = os.path.join(target_subfolder, filename)

                download.save_as(dest_path)

            except Exception as e:
                browser.close()
                raise Exception(str(e))

            browser.close()
            return dest_path, target_subfolder
import requests
import time
import random
import os
from datetime import datetime
from config import UDIFF_CUTOFF_DATE

class MarketFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        self._init_cookies()

    def _init_cookies(self):
        try:
            self.session.get("https://www.nseindia.com", timeout=10)
        except Exception:
            pass 

    def download(self, url, dest_path):
        time.sleep(random.uniform(1.5, 3.5))
        response = self.session.get(url, stream=True, timeout=15)
        if response.status_code == 200:
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        return False

    def fetch_report(self, report_id, target_date, root_dir):
        d_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
        d_MMM = d_obj.strftime("%d%b%Y").upper() 
        d_MM = d_obj.strftime("%d%m%y")          
        d_YYYYMMDD = d_obj.strftime("%Y%m%d")    
        d_DDMMYYYY = d_obj.strftime("%d%m%Y")    
        
        url, filename, exchange, segment = "", "", "", ""

        # ==========================================
        # NSE LOGIC
        # ==========================================
        if report_id.startswith("nse_"):
            exchange = "NSE"
            segment = "Capital_Market" if "cm" in report_id else "Derivatives"
            target_subfolder = os.path.join(root_dir, exchange, segment)

            if report_id == "nse_cm_bhav":
                if d_obj >= UDIFF_CUTOFF_DATE:
                    filename = f"BhavCopy_NSE_CM_0_0_0_{d_YYYYMMDD}_F_0000.csv.zip"
                    url = f"https://nsearchives.nseindia.com/content/cm/{filename}"
                else:
                    filename = f"cm{d_MMM}bhav.csv.zip"
                    url = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{d_obj.year}/{d_obj.strftime('%b').upper()}/{filename}"

            elif report_id == "nse_fo_bhav":
                if d_obj >= UDIFF_CUTOFF_DATE:
                    filename = f"BhavCopy_NSE_FO_0_0_0_{d_YYYYMMDD}_F_0000.csv.zip"
                    url = f"https://nsearchives.nseindia.com/content/fo/{filename}"
                else:
                    filename = f"fo{d_MMM}bhav.csv.zip"
                    url = f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{d_obj.year}/{d_obj.strftime('%b').upper()}/{filename}"

            elif report_id == "nse_cm_pr":
                filename = f"PR{d_MM}.zip"
                url = f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/{filename}"
            elif report_id == "nse_cm_deliv":
                filename = f"MTO_{d_DDMMYYYY}.DAT"
                url = f"https://nsearchives.nseindia.com/archives/equities/mto/{filename}"
            elif report_id == "nse_cm_short":
                filename = f"shortselling_{d_DDMMYYYY}.csv"
                url = f"https://nsearchives.nseindia.com/archives/equities/shortSelling/{filename}"
            elif report_id == "nse_cm_52w":
                filename = f"CM_52_wk_High_low_{d_DDMMYYYY}.csv"
                url = f"https://nsearchives.nseindia.com/content/{filename}"
            elif report_id == "nse_cm_bhav_full":
                filename = f"sec_bhavdata_full_{d_DDMMYYYY}.csv"
                url = f"https://nsearchives.nseindia.com/products/content/{filename}"
            elif report_id == "nse_cm_volt":
                filename = f"CMVOLT_{d_DDMMYYYY}.CSV"
                url = f"https://nsearchives.nseindia.com/archives/nsccl/volt/{filename}"
            elif report_id == "nse_cm_appsec":
                filename = f"APPSEC_COLLVAL_{d_DDMMYYYY}.csv"
                url = f"https://nsearchives.nseindia.com/content/equities/{filename}"
            elif report_id == "nse_cm_mfvar":
                filename = f"MF_VAR_{d_DDMMYYYY}.csv"
                url = f"https://nsearchives.nseindia.com/archives/equities/mf_haircut/{filename}"
            elif report_id == "nse_cm_pe":
                filename = f"PE_{d_MM}.csv"
                url = f"https://nsearchives.nseindia.com/content/equities/peDetail/{filename}"
            elif report_id == "nse_cm_sme":
                filename = f"sme{d_DDMMYYYY}.csv"
                url = f"https://nsearchives.nseindia.com/archives/sme/bhavcopy/{filename}"
            elif report_id == "nse_cm_ma":
                filename = f"MA{d_MM}.csv"
                url = f"https://nsearchives.nseindia.com/archives/equities/mkt/{filename}"
            elif report_id == "nse_cm_var":
                filename = f"C_VAR1_{d_DDMMYYYY}_6.DAT"
                url = f"https://nsearchives.nseindia.com/archives/nsccl/var/{filename}"

        # ==========================================
        # BSE LOGIC
        # ==========================================
        elif report_id.startswith("bse_"):
            exchange = "BSE"
            if "cm" in report_id: segment = "Capital_Market"
            elif "fo" in report_id: segment = "Derivatives"
            else: segment = "Debt"
            target_subfolder = os.path.join(root_dir, exchange, segment)

            if report_id == "bse_cm_bhav":
                if d_obj >= UDIFF_CUTOFF_DATE:
                    filename = f"BhavCopy_BSE_CM_0_0_0_{d_YYYYMMDD}_F_0000.CSV"
                    url = f"https://www.bseindia.com/download/BhavCopy/Equity/{filename}"
                else:
                    filename = f"EQ{d_MM}_CSV.ZIP"
                    url = f"https://www.bseindia.com/download/BhavCopy/Equity/{filename}"
                    
            elif report_id == "bse_fo_bhav":
                if d_obj >= UDIFF_CUTOFF_DATE:
                    filename = f"BhavCopy_BSE_FO_0_0_0_{d_YYYYMMDD}_F_0000.CSV"
                    url = f"https://www.bseindia.com/download/Bhavcopy/Derivative/{filename}"
                else:
                    filename = f"bhavcopy{d_obj.strftime('%d-%m-%y')}.zip"
                    url = f"https://www.bseindia.com/download/Bhavcopy/Derivative/{filename}"

            elif report_id == "bse_cm_slb":
                filename = f"SQ{d_MM}_CSV.ZIP"
                url = f"https://www.bseindia.com/download/Bhavcopy/SLB/{filename}"
                
            elif report_id == "bse_debt_bhav":
                filename = f"DEBTBHAVCOPY{d_DDMMYYYY}.zip"
                url = f"https://www.bseindia.com/download/Bhavcopy/Debt/{filename}"
                
            elif report_id == "bse_cm_index":
                filename = f"INDEXSummary_{d_DDMMYYYY}.csv"
                url = f"https://www.bseindia.com/bsedata/Index_Bhavcopy/{filename}"

        else:
            raise ValueError(f"Report ID {report_id} not mapped.")

        os.makedirs(target_subfolder, exist_ok=True)
        dest_path = os.path.join(target_subfolder, filename)
        
        success = self.download(url, dest_path)
        if not success:
            # FIX: Clean error message without holiday assumptions
            raise Exception("HTTP 404 / Connection Timeout")
            
        return dest_path, exchange, segment, target_subfolder
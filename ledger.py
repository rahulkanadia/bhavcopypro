import json
import os

LEDGER_FILE = "download_ledger.json"

def load_ledger():
    if not os.path.exists(LEDGER_FILE):
        return {}
    with open(LEDGER_FILE, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}

def save_ledger(data):
    with open(LEDGER_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def check_exists(ledger, date_str, report_id):
    key = f"{date_str}_{report_id}"
    return key in ledger and ledger[key].get("status") == "SUCCESS"

def record_download(date_str, report_id, status, path):
    ledger = load_ledger()
    key = f"{date_str}_{report_id}"
    ledger[key] = {
        "date": date_str,
        "report_id": report_id,
        "status": status,
        "path": path
    }
    save_ledger(ledger)
from datetime import date

# The SEBI Mandate Transition Date
UDIFF_CUTOFF_DATE = date(2024, 7, 8)

# Update this with your exact PostgreSQL credentials
DB_URI = "postgresql://postgres:postgres@localhost:5432/nse_archive"

CONVERSATIONAL_MAP = {
    "corpbond": "corporate_bonds",
    "C_VAR1": "var_margin_parameters",
    "shortselling": "short_selling",
    "MTO": "delivery_positions",
    "CMVOLT": "daily_volatility",
    "APPSEC_COLLVAL": "approved_securities_haircut",
    "MF_VAR": "mutual_funds_haircut",
    "sec_bhavdata_full": "sec_bhavdata_full",
    "CM_52_wk_High_low": "52w_high_low",
    "PE": "pe_ratio",
    "sme": "sme_bhavcopy",
    "MA": "market_activity",
    "cm": "cm_bhavcopy",
    "fo": "fo_bhavcopy",
    "BhavCopy_NSE_CM": "cm_udiff_bhavcopy",
    "BhavCopy_NSE_FO": "fo_udiff_bhavcopy",
    # Legacy PR Archive internal mappings
    "bc": "corporate_actions",
    "hl": "52w_high_low",
    "tt": "top_25_traded"
}

# Table definitions for ON CONFLICT DO UPDATE
TABLE_KEYS = {
    "sec_bhavdata_full": ["trade_date", "symbol", "series"],
    "cm_bhavcopy": ["trade_date", "symbol", "series"],
    "pe_ratio": ["trade_date", "symbol"],
    "daily_volatility": ["trade_date", "symbol"],
    "corporate_bonds": ["trade_date", "symbol", "series"],
    "sme_bhavcopy": ["trade_date", "symbol", "series"],
    "52w_high_low": ["trade_date", "symbol", "series"],
    "delivery_positions": ["trade_date", "record_type", "security_code"],
    "corporate_actions": ["trade_date", "symbol"],
    "top_25_traded": ["trade_date", "security"],
    "cm_udiff_bhavcopy": ["trade_date", "tckrsymb", "sctysrs"]
}

REPORT_TREE = {
    "NSE": {
        "Capital Market": [
            {"id": "nse_cm_bhav", "name": "Standard Bhavcopy (EOD)"},
            {"id": "nse_cm_bhav_full", "name": "Full Bhavcopy & Delivery (sec_bhavdata)"},
            {"id": "nse_cm_pr", "name": "PR Archive (Full Zip)"},
            {"id": "nse_cm_deliv", "name": "Delivery Positions (MTO)"},
            {"id": "nse_cm_short", "name": "Short Selling"},
            {"id": "nse_cm_var", "name": "VaR Margin Parameters (EOD)"},
            {"id": "nse_cm_volt", "name": "Daily Volatility (CMVOLT)"},
            {"id": "nse_cm_52w", "name": "52 Week High Low"},
            {"id": "nse_cm_appsec", "name": "Approved Securities Haircut"},
            {"id": "nse_cm_mfvar", "name": "Mutual Funds Haircut"},
            {"id": "nse_cm_pe", "name": "PE Ratio"},
            {"id": "nse_cm_sme", "name": "SME Bhavcopy"},
            {"id": "nse_cm_ma", "name": "Market Activity Report"},
        ],
        "Derivatives": [
            {"id": "nse_fo_bhav", "name": "F&O Bhavcopy"},
        ]
    }
    # BSE Block Compartmentalized For Phase 1
}
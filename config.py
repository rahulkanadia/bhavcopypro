from datetime import date

# The SEBI Mandate Transition Date
UDIFF_CUTOFF_DATE = date(2024, 7, 8)

CONVERSATIONAL_MAP = {
    "corpbond": "Corporate_Bonds",
    "C_VAR1": "VaR_Margin_Parameters",
    "shortselling": "Short_Selling",
    "MTO": "Delivery_Positions",
    "CMVOLT": "Daily_Volatility",
    "APPSEC_COLLVAL": "Approved_Securities_Haircut",
    "MF_VAR": "Mutual_Funds_Haircut",
    "sec_bhavdata_full": "Full_Bhavcopy_Delivery",
    "CM_52_wk_High_low": "52W_High_Low",
    "PE": "PE_Ratio",
    "sme": "SME_Bhavcopy",
    "MA": "Market_Activity",
    "INDEXSummary": "Index_Summary",
    "DEBTBHAVCOPY": "Debt_Bhavcopy",
    "SQ": "SLB_Bhavcopy",
    "cm": "CM_Bhavcopy",
    "fo": "FO_Bhavcopy",
    "PR": "Price_Report_Archive",
    "EQ": "BSE_Equity_Bhavcopy",
    "BhavCopy_NSE_CM": "CM_UDiFF_Bhavcopy",
    "BhavCopy_NSE_FO": "FO_UDiFF_Bhavcopy",
    "BhavCopy_BSE_CM": "BSE_CM_UDiFF_Bhavcopy",
    "BhavCopy_BSE_FO": "BSE_FO_UDiFF_Bhavcopy",
    "BhavCopy_BSE_CD": "BSE_Currency_UDiFF",
    "BhavCopy_BSE_CO": "BSE_Commodity_UDiFF",
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
    },
    "BSE": {
        "Capital Market": [
            {"id": "bse_cm_bhav", "name": "BSE Equity Bhavcopy"},
            {"id": "bse_cm_slb", "name": "SLB Bhavcopy"},
            {"id": "bse_cm_index", "name": "Index Summary"},
        ],
        "Derivatives": [
            {"id": "bse_fo_bhav", "name": "BSE Equity Derivatives"},
        ],
        "Debt": [
            {"id": "bse_debt_bhav", "name": "Debt Bhavcopy"},
        ]
    }
}
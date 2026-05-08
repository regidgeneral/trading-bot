"""
setup_sheets.py — Chạy 1 lần trên máy tính để tạo các tab cần thiết trong Google Sheets.

Cách dùng:
  pip install gspread google-auth
  python setup_sheets.py
"""
import json
import gspread
from google.oauth2.service_account import Credentials

CREDENTIALS_FILE = r"credentials.json"   # ← sửa đường dẫn
GOOGLE_SHEET_ID  = "your_sheet_id_here"  # ← sửa Sheet ID

SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
gc    = gspread.authorize(creds)
sh    = gc.open_by_key(GOOGLE_SHEET_ID)

tabs = {
    "orders":    ["Date", "Symbol", "Side", "Qty", "Price", "Status", "OrderID", "Strategy"],
    "positions": ["Date", "Symbol", "EntryPrice", "Qty", "StopLoss", "TakeProfit", "Status", "PnL"],
    "pnl":       ["Date", "RealizedPnL", "TotalTrades", "WinTrades"],
}

existing = [ws.title for ws in sh.worksheets()]

for tab_name, headers in tabs.items():
    if tab_name not in existing:
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
        ws.append_row(headers)
        print(f"✅ Tab '{tab_name}' created")
    else:
        print(f"⏭️  Tab '{tab_name}' already exists — skip")

print("\nDone! Bây giờ có thể deploy lên Railway.")

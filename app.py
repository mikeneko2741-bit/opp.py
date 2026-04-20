import streamlit as st
import pandas as pd
import os
from datetime import datetime
import uuid
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import quote
import time
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import streamlit.components.v1 as components

# ---------------------------------------------------------
# ⚙️ 設定・定数 (v5.7)
# ---------------------------------------------------------
JSON_KEY_FILE = 'secrets.json'
SPREADSHEET_NAME = 'ぽっけぇ〜道_システムv3'

SHEET_INVENTORY = '在庫DB'
SHEET_PURCHASE = '仕入帳'
SHEET_SALES = '売上帳'
SHEET_CART = 'カート下書き'

# ---------------------------------------------------------
# 📷 スマホ内蔵カメラ用 QRスキャナー部品 (v5.7 レイアウトかぶり防止策)
# ---------------------------------------------------------
QR_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <script src="https://unpkg.com/html5-qrcode" type="text/javascript"></script>
</head>
<body style="margin:0; padding:5px; font-family:sans-serif; background:#f0f2f6; min-height:400px; display:flex; flex-direction:column;">
  <div id="reader" style="width:100%; max-width:500px; margin:0 auto; border-radius:8px; overflow:hidden; border:1px solid #ddd; background:#fff; flex-grow:1; min-height:350px;"></div>
  <script>
    let scannedIds = [];
    let validIds = [];
    
    const AudioContext = window.AudioContext || window.webkitAudioContext;
    const ctx = new AudioContext();
    function playBeep(type) {
        if (ctx.state === 'suspended') ctx.resume();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        if (type === 'success') {
            osc.type = 'sine'; osc.frequency.setValueAtTime(880, ctx.currentTime);
            gain.gain.setValueAtTime(0.1, ctx.currentTime);
            osc.start(); gain.gain.exponentialRampToValueAtTime(0.00001, ctx.currentTime + 0.1); osc.stop(ctx.currentTime + 0.1);
        } else {
            osc.type = 'square'; osc.frequency.setValueAtTime(150, ctx.currentTime);
            gain.gain.setValueAtTime(0.1, ctx.currentTime);
            osc.start(); gain.gain.exponentialRampToValueAtTime(0.00001, ctx.currentTime + 0.3); osc.stop(ctx.currentTime + 0.3);
        }
    }

    function sendValue(val) {
      window.parent.postMessage({ isStreamlitMessage: true, type: "streamlit:setComponentValue", value: {id: val, ts: Date.now()} }, "*");
    }

    function updateHeight() {
      let h = document.body.scrollHeight + 20;
      if (h < 420) h = 420;
      window.parent.postMessage({ isStreamlitMessage: true, type: "streamlit:setFrameHeight", height: h }, "*");
    }

    function onDataFromPython(event) {
        if (event.data.type !== "streamlit:render") return;
        if (event.data.args.scanned_ids) scannedIds = event.data.args.scanned_ids;
        if (event.data.args.valid_ids) validIds = event.data.args.valid_ids;
        updateHeight();
    }
    window.addEventListener("message", onDataFromPython);

    window.onload = function() {
      window.parent.postMessage({ isStreamlitMessage: true, type: "streamlit:componentReady", apiVersion: 1 }, "*");
      
      const observer = new MutationObserver(updateHeight);
      observer.observe(document.body, { childList: true, subtree: true, attributes: true });
      window.addEventListener('resize', updateHeight);

      let lastScanned = "";
      let scanner = new Html5QrcodeScanner("reader", { fps: 10, qrbox: {width: 250, height: 250}, aspectRatio: 1.0 }, false);
      scanner.render(function(txt) {
        if (txt !== lastScanned) {
            lastScanned = txt;
            if (validIds.length > 0 && !validIds.includes(txt)) playBeep('error');
            else if (scannedIds.includes(txt)) playBeep('error');
            else playBeep('success');
            sendValue(txt);
            setTimeout(() => { lastScanned = ""; }, 1500);
        }
      });
      setTimeout(updateHeight, 500);
    };
  </script>
</body>
</html>
"""

def get_camera_qr_scanner():
    comp_dir = os.path.join(os.path.dirname(__file__), "qr_cam_comp")
    os.makedirs(comp_dir, exist_ok=True)
    idx_path = os.path.join(comp_dir, "index.html")
    with open(idx_path, "w", encoding="utf-8") as f: f.write(QR_HTML)
    return components.declare_component("camera_qr_scanner", path=comp_dir)

_scanner = get_camera_qr_scanner()

# ---------------------------------------------------------
# 🔌 データベース接続＆初期化機能
# ---------------------------------------------------------
@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        if "gcp_service_account" in st.secrets: key_dict = st.secrets["gcp_service_account"]
        elif "private_key" in st.secrets: key_dict = st.secrets
        elif os.path.exists(JSON_KEY_FILE):
            creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
            return gspread.authorize(creds)
        else: return None
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        return gspread.authorize(creds)
    except Exception: return None

def get_spreadsheet():
    client = get_gspread_client()
    if client:
        try: return client.open(SPREADSHEET_NAME)
        except Exception: return None
    return None

def check_and_init_sheets():
    sh = get_spreadsheet()
    if not sh: return None, None, None, None
    try: ws_inv = sh.worksheet(SHEET_INVENTORY)
    except:
        ws_inv = sh.add_worksheet(title=SHEET_INVENTORY, rows=1000, cols=17)
        ws_inv.append_row(['ID', '商品名', '収録パック', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号', '相場更新', '重量', '個別メモ'])
    try: ws_pur = sh.worksheet(SHEET_PURCHASE)
    except:
        ws_pur = sh.add_worksheet(title=SHEET_PURCHASE, rows=1000, cols=14)
        ws_pur.append_row(['ID', '仕入日', '仕入名目', '商品名', '収録パック', '種類', '状態_PSA', '数量', '単価', '小計', '仕入先', '備考', '登録日時'])
    try: ws_sales = sh.worksheet(SHEET_SALES)
    except:
        ws_sales = sh.add_worksheet(title=SHEET_SALES, rows=1000, cols=15)
        ws_sales.append_row(['ID', '元の在庫ID', '売却日', '商品名', '収録パック', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時'])
    try: ws_cart = sh.worksheet(SHEET_CART)
    except:
        ws_cart = sh.add_worksheet(title=SHEET_CART, rows=1000, cols=3)
        ws_cart.append_row(['SessionID', 'Timestamp', 'CartJSON'])
    return ws_inv, ws_pur, ws_sales, ws_cart

@st.cache_data(ttl=60)
def load_data():
    ws_inv, _, _, _ = check_and_init_sheets()
    if ws_inv:
        try:
            df = get_as_dataframe(ws_inv, evaluate_formulas=True)
            df = df.dropna(subset=['ID'])
            df = df[df['ID'] != '']
            for c in ['PSA番号', '収録パック', '重量', '個別メモ']:
                if c not in df.columns: df[c] = ""
                df[c] = df[c].astype(str).replace('nan', '')
            if '状態_PSA' not in df.columns: df['状態_PSA'] = "A (美品)"
            if '相場更新' not in df.columns: df['相場更新'] = True
            else:
                df['相場更新'] = df['相場更新'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False, '1': True, '0': False})
                df['相場更新'] = df['相場更新'].fillna(True).astype(bool)
            for c in ['原価', '参考相場', '在庫数']:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
            return df
        except Exception: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    ws_inv, _, _, _ = check_and_init_sheets()
    if not ws_inv: return df
    save_cols = ['ID', '商品名', '収録パック', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号', '相場更新', '重量', '個別メモ']
    df_to_save = df.copy()
    for col in save_cols:
        if col not in df_to_save.columns: df_to_save[col] = True if col == '相場更新' else ""
    df_to_save = df_to_save[save_cols]
    df_ex = get_as_dataframe(ws_inv, evaluate_formulas=False)
    df_ex = df_ex.dropna(how='all')
    if df_ex.empty:
        df_ex = pd.DataFrame(columns=save_cols)
        df_ex['__row'] = pd.Series(dtype=int)
    else: df_ex['__row'] = df_ex.index + 2  
    df_ex = df_ex.dropna(subset=['ID'])
    df_ex = df_ex[df_ex['ID'] != '']
    ex_cols = [c for c in save_cols if c in df_ex.columns]
    df_ex = df_ex[['ID', '__row'] + [c for c in ex_cols if c != 'ID']]
    merged = pd.merge(df_ex, df_to_save, on='ID', how='outer', suffixes=('_old', ''), indicator=True)
    cells_to_update = []
    max_row = int(df_ex['__row'].max()) if not df_ex.empty else 1
    next_new_row = max_row + 1
    for _, row in merged.iterrows():
        status = row['_merge']
        if status == 'both':
            r = int(row['__row'])
            for c_idx, col in enumerate(save_cols):
                old_val = row.get(f"{col}_old", None)
                new_val = row[col]
                s_old = "" if pd.isna(old_val) else str(old_val).strip()
                s_new = "" if pd.isna(new_val) else str(new_val).strip()
                try:
                    if s_old and s_new and float(s_old) == float(s_new): continue
                except ValueError: pass
                if s_old != s_new:
                    val = "" if pd.isna(new_val) else new_val
                    cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=val))
        elif status == 'right_only':
            r = next_new_row
            next_new_row += 1
            for c_idx, col in enumerate(save_cols):
                val = row[col]
                if pd.notna(val) and val != "": cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=val))
        elif status == 'left_only':
            r = int(row['__row'])
            for c_idx in range(len(save_cols)): cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=""))
    if cells_to_update:
        for attempt in range(3):
            try:
                ws_inv.update_cells(cells_to_update)
                break
            except Exception as e:
                if attempt == 2: raise e
                time.sleep(2 ** attempt)
    return df_to_save

@st.cache_data(ttl=60)
def load_sales_data():
    _, _, ws_sales, _ = check_and_init_sheets()
    if ws_sales:
        try:
            df = get_as_dataframe(ws_sales, evaluate_formulas=True)
            df = df.dropna(subset=['ID'])
            df = df[df['ID'] != '']
            if '元の在庫ID' not in df.columns: df['元の在庫ID'] = ""
            if '収録パック' not in df.columns: df['収録パック'] = ""
            if '状態_PSA' not in df.columns: df['状態_PSA'] = df['商品名'].astype(str).apply(lambda x: '-' if 'オリパ' in x or 'サプライ' in x else 'A (美品)')
            for col in ['売却数', '売上額', '手数料', '経費_送料', '純利益']: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            return df
        except Exception: return pd.DataFrame()
    return pd.DataFrame()

def save_sales_data(df):
    _, _, ws_sales, _ = check_and_init_sheets()
    if not ws_sales: return df
    save_cols = ['ID', '元の在庫ID', '売却日', '商品名', '収録パック', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時']
    df_to_save = df.copy()
    for col in save_cols:
        if col not in df_to_save.columns: df_to_save[col] = ""
    df_to_save = df_to_save[save_cols]
    df_ex = get_as_dataframe(ws_sales, evaluate_formulas=False)
    df_ex = df_ex.dropna(how='all')
    if df_ex.empty:
        df_ex = pd.DataFrame(columns=save_cols)
        df_ex['__row'] = pd.Series(dtype=int)
    else: df_ex['__row'] = df_ex.index + 2
    df_ex = df_ex.dropna(subset=['ID'])
    df_ex = df_ex[df_ex['ID'] != '']
    ex_cols = [c for c in save_cols if c in df_ex.columns]
    df_ex = df_ex[['ID', '__row'] + [c for c in ex_cols if c != 'ID']]
    merged = pd.merge(df_ex, df_to_save, on='ID', how='outer', suffixes=('_old', ''), indicator=True)
    cells_to_update = []
    max_row = int(df_ex['__row'].max()) if not df_ex.empty else 1
    next_new_row = max_row + 1
    for _, row in merged.iterrows():
        status = row['_merge']
        if status == 'both':
            r = int(row['__row'])
            for c_idx, col in enumerate(save_cols):
                old_val = row.get(f"{col}_old", None)
                new_val = row[col]
                s_old = "" if pd.isna(old_val) else str(old_val).strip()
                s_new = "" if pd.isna(new_val) else str(new_val).strip()
                if s_old != s_new:
                    val = "" if pd.isna(new_val) else new_val
                    cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=val))
        elif status == 'right_only':
            r = next_new_row
            next_new_row += 1
            for c_idx, col in enumerate(save_cols):
                val = row[col]
                if pd.notna(val) and val != "": cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=val))
        elif status == 'left_only':
            r = int(row['__row'])
            for c_idx in range(len(save_cols)): cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=""))
    if cells_to_update:
        for attempt in range(3):
            try:
                ws_sales.update_cells(cells_to_update)
                break
            except Exception as e:
                if attempt == 2: raise e
                time.sleep(2 ** attempt)
    return df_to_save

@st.cache_data(ttl=60)
def load_purchase_data():
    _, ws_pur, _, _ = check_and_init_sheets()
    if ws_pur:
        try:
            df = get_as_dataframe(ws_pur, evaluate_formulas=True)
            df = df.dropna(subset=['ID'])
            df = df[df['ID'] != '']
            if '収録パック' not in df.columns: df['収録パック'] = ""
            if '状態_PSA' not in df.columns:
                if '種類' in df.columns: df['状態_PSA'] = df['種類'].apply(lambda x: '-' if x in ['オリジナルパック', 'サプライ'] else 'A (美品)')
                else: df['状態_PSA'] = 'A (美品)'
            for col in ['数量', '単価', '小計']:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            return df
        except Exception: return pd.DataFrame()
    return pd.DataFrame()

def save_purchase_data(df):
    _, ws_pur, _, _ = check_and_init_sheets()
    if not ws_pur: return df
    save_cols = ['ID', '仕入日', '仕入名目', '商品名', '収録パック', '種類', '状態_PSA', '数量', '単価', '小計', '仕入先', '備考', '登録日時']
    df_to_save = df.copy()
    for col in save_cols:
        if col not in df_to_save.columns: df_to_save[col] = ""
    df_to_save = df_to_save[save_cols]
    df_ex = get_as_dataframe(ws_pur, evaluate_formulas=False)
    df_ex = df_ex.dropna(how='all')
    if df_ex.empty:
        df_ex = pd.DataFrame(columns=save_cols)
        df_ex['__row'] = pd.Series(dtype=int)
    else: df_ex['__row'] = df_ex.index + 2
    df_ex = df_ex.dropna(subset=['ID'])
    df_ex = df_ex[df_ex['ID'] != '']
    ex_cols = [c for c in save_cols if c in df_ex.columns]
    df_ex = df_ex[['ID', '__row'] + [c for c in ex_cols if c != 'ID']]
    merged = pd.merge(df_ex, df_to_save, on='ID', how='outer', suffixes=('_old', ''), indicator=True)
    cells_to_update = []
    max_row = int(df_ex['__row'].max()) if not df_ex.empty else 1
    next_new_row = max_row + 1
    for _, row in merged.iterrows():
        status = row['_merge']
        if status == 'both':
            r = int(row['__row'])
            for c_idx, col in enumerate(save_cols):
                old_val = row.get(f"{col}_old", None)
                new_val = row[col]
                s_old = "" if pd.isna(old_val) else str(old_val).strip()
                s_new = "" if pd.isna(new_val) else str(new_val).strip()
                if s_old != s_new:
                    val = "" if pd.isna(new_val) else new_val
                    cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=val))
        elif status == 'right_only':
            r = next_new_row
            next_new_row += 1
            for c_idx, col in enumerate(save_cols):
                val = row[col]
                if pd.notna(val) and val != "": cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=val))
        elif status == 'left_only':
            r = int(row['__row'])
            for c_idx in range(len(save_cols)): cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=""))
    if cells_to_update:
        for attempt in range(3):
            try:
                ws_pur.update_cells(cells_to_update)
                break
            except Exception as e:
                if attempt == 2: raise e
                time.sleep(2 ** attempt)
    return df_to_save

def record_purchase_items(batch_id, date, title, source, note, items):
    _, ws_pur, _, _ = check_and_init_sheets()
    if ws_pur:
        rows = []
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for item in items:
            row = [f"{batch_id}-{str(uuid.uuid4())[:4]}", date, title, item['name'], item.get('pack', ''), item['type'], item.get('cond', 'A (美品)'), item['qty'], item['unit_cost'], item['subtotal'], source, note, now_str]
            rows.append(row)
        if rows:
            for attempt in range(3):
                try:
                    ws_pur.append_rows(rows)
                    break
                except Exception as e:
                    if attempt == 2: raise e
                    time.sleep(2 ** attempt)
            load_purchase_data.clear()

def save_cart_draft(session_id, cart_data):
    _, _, _, ws_cart = check_and_init_sheets()
    if ws_cart:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cart_json = json.dumps(cart_data, ensure_ascii=False)
        try:
            cell = ws_cart.find(session_id, in_column=1)
            ws_cart.update(f'B{cell.row}:C{cell.row}', [[now_str, cart_json]])
        except Exception: ws_cart.append_row([session_id, now_str, cart_json])

def load_cart_draft(session_id):
    _, _, _, ws_cart = check_and_init_sheets()
    if ws_cart:
        try:
            cell = ws_cart.find(session_id, in_column=1)
            row_data = ws_cart.row_values(cell.row)
            if len(row_data) >= 3: return json.loads(row_data[2])
        except Exception: return []
    return []

def recalculate_moving_average_costs():
    df_inv, df_pur, df_sales = load_data(), load_purchase_data(), load_sales_data()
    if df_inv.empty or df_pur.empty: return df_inv
    history, events = {}, []
    for _, row in df_pur.iterrows():
        dt = pd.to_datetime(row['登録日時'], errors='coerce')
        if pd.isna(dt): dt = datetime.min
        events.append({'time': dt, 'type_priority': 0, 'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(), 'cond': str(row.get('状態_PSA', 'A (美品)')).strip(), 'qty': int(row['数量']), 'subtotal': int(row['小計'])})
    for _, row in df_sales.iterrows():
        dt = pd.to_datetime(row['登録日時'], errors='coerce')
        if pd.isna(dt): dt = datetime.min
        events.append({'time': dt, 'type_priority': 1, 'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(), 'cond': str(row.get('状態_PSA', 'A (美品)')).strip(), 'qty': int(row['売却数'])})
    events.sort(key=lambda x: (x['time'], x['type_priority']))
    for ev in events:
        key = (ev['name'], ev['pack'], ev['cond'])
        if key not in history: history[key] = {'qty': 0, 'cost': 0}
        state = history[key]
        if ev['type_priority'] == 0:
            new_qty = state['qty'] + ev['qty']
            total_val = (state['qty'] * state['cost']) + ev['subtotal']
            state['cost'] = int(total_val / new_qty) if new_qty > 0 else 0
            state['qty'] = new_qty
        elif ev['type_priority'] == 1:
            state['qty'] -= ev['qty']
            if state['qty'] < 0: state['qty'] = 0
    for idx, row in df_inv.iterrows():
        key = (str(row['商品名']).strip(), str(row.get('収録パック', '')).strip(), str(row.get('状態_PSA', 'A (美品)')).strip())
        if key in history: df_inv.at[idx, '原価'] = history[key]['cost']
    return df_inv

def encrypt_cost(cost):
    cost_str = str(int(cost))
    if not cost_str: return ""
    mapping = {'1':'A', '2':'B', '3':'C', '4':'D', '5':'E', '6':'F', '7':'G', '8':'H', '9':'I', '0':'J'}
    return mapping.get(cost_str[0], cost_str[0]) + cost_str[1:]

def generate_label_html(items, start_pos=1):
    html = """<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8"><title>ぽっけぇ〜道 管理ラベル</title><style>@media print { @page { margin: 0; } body { margin: 0; } } body { font-family: sans-serif; margin: 0; padding: 0; background: #fff; } .page { width: 210mm; min-height: 297mm; padding: 12mm 4mm; margin: 0 auto; box-sizing: border-box; display: grid; grid-template-columns: repeat(3, 1fr); grid-auto-rows: 33.9mm; gap: 0; page-break-after: always; } .label { padding: 3mm; box-sizing: border-box; display: flex; align-items: center; overflow: hidden; border: 1px dashed #eee; } .empty-label { padding: 3mm; box-sizing: border-box; border: 1px dashed transparent; } .qr-code { width: 20mm; height: 20mm; flex-shrink: 0; } .details { margin-left: 3mm; font-size: 8pt; line-height: 1.2; width: 100%; overflow: hidden; display: flex; flex-direction: column; justify-content: space-between; height: 100%; } .id { font-size: 10pt; font-weight: bold; margin-bottom: 2px; } .name { font-weight: bold; font-size: 9pt; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 2px; } .memo { font-size: 7.5pt; color: #444; line-height: 1.1; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; flex-grow: 1; } .bottom-row { display: flex; justify-content: space-between; align-items: flex-end; font-size: 7pt; color: #333; margin-top: auto; } .enc-cost { font-weight: bold; }</style><script>window.onload = function() { window.print(); }</script></head><body><div class="page">"""
    for _ in range(start_pos - 1): html += '<div class="empty-label"></div>'
    for item in items:
        enc_cost = encrypt_cost(item.get('原価', 0))
        weight = f" / {item.get('重量', '')}g" if item.get('重量') else ""
        memo = item.get('個別メモ', '')
        memo_html = f'<div class="memo">{memo}</div>' if memo else '<div class="memo"></div>'
        html += f"""<div class="label"><img class="qr-code" src="https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={item['ID']}"><div class="details"><div class="id">{item['ID']}</div><div class="name">{item['商品名']}</div>{memo_html}<div class="bottom-row"><span>{item['状態_PSA']}{weight}</span><span class="enc-cost">{enc_cost}</span></div></div></div>"""
    html += "</div></body></html>"
    return html

def clean_product_name(text):
    if not isinstance(text, str): return str(text)
    return re.sub(r'\{-}.*$', '', text).strip()

def generate_search_keyword(orig_name):
    is_box = "BOX" in orig_name.upper() or "ｂｏｘ" in orig_name.lower()
    match = re.search(r'『(.+?)』', orig_name)
    base = match.group(1) if match else orig_name
    cleaned = re.sub(r'\[.*?\]|\(.*?\)|【.*?】', '', base).strip()
    cleaned = cleaned.replace("拡張パック", "").replace("強化", "").replace("ハイクラスパック", "").replace("構築済みデッキ", "").strip()
    if is_box and "BOX" not in cleaned.upper(): cleaned += " BOX"
    return cleaned.strip() if cleaned else orig_name.strip()

def get_best_match(orig_name, orig_pack, results):
    ng = ["キズ", "傷", "イタミ", "ダメージ", "シュリンクなし", "シュリンク破れ", "特価", "難あり", "訳あり", "ジャンク", "開封済"]
    sp = ["デラックス", "スペシャル", "プレミアム", "セット", "ジャンボ", "コレクション", "クラシック"]
    for res in results:
        if any(n in res['name'] for n in ng): continue
        if orig_pack and res['pack'] and orig_pack.upper() != res['pack'].upper(): continue
        is_safe = True
        for s in sp:
            if s not in orig_name and s in res['name']: is_safe = False; break
        if not is_safe: continue
        return res
    return None

def fetch_from_url(url):
    results = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"}
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.select('.item_box, .goods_box, .item_data, .sys_item_row, .search_result_item')
        for item in items:
            name_tag = item.select_one('.item_name, .goods_name, .name')
            if not name_tag: continue
            raw_name = name_tag.get_text(strip=True)
            pack_code = ""
            pack_match = re.search(r'\[([a-zA-Z0-9-]+)\]', raw_name)
            if pack_match: pack_code = pack_match.group(1)
            is_box = "BOX" in raw_name.upper() or "ｂｏｘ" in raw_name.lower()
            clean_name = clean_product_name(raw_name)
            if is_box and "BOX" not in clean_name.upper(): clean_name = f"{clean_name} BOX"
            price = 0
            price_tag = item.select_one('.figure, .price, .goods_price')
            if price_tag:
                nums = re.findall(r'\d+', price_tag.get_text(strip=True).replace(',', ''))
                if nums: price = int(nums[0])
            img_url = ""
            img_tags = item.select('img')
            for img in img_tags:
                temp_url = ""
                for attr in ['data-original', 'data-src', 'src']:
                    if attr in img.attrs and img[attr]: temp_url = img[attr]; break
                if temp_url:
                    if any(bad in temp_url.lower() for bad in ["spacer", "blank", "icon", "ranking", "mark", "sold"]): continue
                    img_url = temp_url; break
            if img_url.startswith('/'): img_url = "https://www.cardrush-pokemon.jp" + img_url
            product_url = ""
            a_tag = item.select_one('a[href]')
            if a_tag:
                product_url = a_tag['href']
                if product_url.startswith('/'): product_url = "https://www.cardrush-pokemon.jp" + product_url
            if price > 0: results.append({"name": clean_name, "pack": pack_code, "price": price, "image": img_url, "url": product_url})
        unique = []
        seen = set()
        for r in results:
            id_val = r['url'] if r['url'] else r['name']
            if id_val not in seen: unique.append(r); seen.add(id_val)
        return unique
    except Exception: return []

def search_card_rush(keyword):
    encoded = quote(keyword.encode('utf-8'))
    url_a = f"https://www.cardrush-pokemon.jp/product-list?keyword={encoded}&num=50"
    results = fetch_from_url(url_a)
    if not results:
        url_b = f"https://www.cardrush-pokemon.jp/shop/shopbrand.html?search={encoded}"
        results = fetch_from_url(url_b)
    return results

# ---------------------------------------------------------
# 🖥️ アプリ画面 (v5.7)
# ---------------------------------------------------------
st.set_page_config(page_title="ぽっけぇ～道 システム", layout="wide")
st.title("🎴 ぽっけぇ～道 管理システム v5.7")

if 'session_id' not in st.session_state: st.session_state['session_id'] = str(uuid.uuid4())
if 'cart' not in st.session_state: st.session_state['cart'] = []
if 'has_searched' not in st.session_state: st.session_state['has_searched'] = False
if 'reset_key' not in st.session_state: st.session_state['reset_key'] = 0
if 'oripa_scanned' not in st.session_state: st.session_state['oripa_scanned'] = []
if 'sell_cart' not in st.session_state: st.session_state['sell_cart'] = []

if 'phys_scan_val_sell' not in st.session_state: st.session_state['phys_scan_val_sell'] = ""
if 'phys_scan_pend_sell' not in st.session_state: st.session_state['phys_scan_pend_sell'] = None
def cb_phys_sell():
    if st.session_state['phys_scan_val_sell']:
        st.session_state['phys_scan_pend_sell'] = st.session_state['phys_scan_val_sell']
        st.session_state['phys_scan_val_sell'] = ""

if 'phys_scan_val_oripa' not in st.session_state: st.session_state['phys_scan_val_oripa'] = ""
if 'phys_scan_pend_oripa' not in st.session_state: st.session_state['phys_scan_pend_oripa'] = None
def cb_phys_oripa():
    if st.session_state['phys_scan_val_oripa']:
        st.session_state['phys_scan_pend_oripa'] = st.session_state['phys_scan_val_oripa']
        st.session_state['phys_scan_val_oripa'] = ""

menu = st.sidebar.radio("【作業メニュー】", ["📦 スピード仕入・解体", "📊 在庫・PSA管理", "🖨️ 個別管理・ラベル", "🛍️ オリパ工場", "📖 帳簿・分析"])

# =========================================================
# 📦 第1フェーズ：スピード仕入・解体
# =========================================================
if menu == "📦 スピード仕入・解体":
    st.header("📦 スピード仕入・福袋解体 (カート式)")
    col_left, col_right = st.columns([1.2, 1])
    with col_left:
        st.subheader("① 商品を探してカートに入れる")
        tab_search, tab_manual, tab_bulk, tab_supply = st.tabs(["🔍 検索", "✍️ 手動登録", "🗃️ 素材", "📦 サプライ"])
        with tab_search:
            search_word = st.text_input("カード名・BOX名を入力")
            if st.button("検索", type="primary", use_container_width=True):
                if search_word:
                    with st.spinner("検索中..."):
                        try:
                            st.session_state['search_res'] = search_card_rush(search_word)
                            st.session_state['has_searched'] = True
                        except Exception: st.session_state['has_searched'] = False
            if st.session_state.get('has_searched') and st.session_state.get('search_res'):
                sort_order = st.selectbox("並び替え", ["価格の高い順", "価格の安い順", "おすすめ順"])
                display_res = list(st.session_state['search_res'])
                if sort_order == "価格の高い順": display_res.sort(key=lambda x: x['price'], reverse=True)
                elif sort_order == "価格の安い順": display_res.sort(key=lambda x: x['price'])
                for item in display_res:
                    c1, c2, c3 = st.columns([1, 3, 2])
                    with c1: st.image(item['image'], width=50) if item['image'] else st.write("🖼️")
                    with c2: st.write(f"**{item['name']}** [{item['pack']}]"); st.caption(f"相場: ¥{item['price']:,}")
                    with c3:
                        with st.popover("カートに追加"):
                            qty = st.number_input("数量", min_value=1, value=1, key=f"q_{item['name']}")
                            cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封"], key=f"c_{item['name']}")
                            if st.button("追加", key=f"a_{item['name']}"):
                                st.session_state['cart'].append({"id": str(uuid.uuid4())[:8], "name": item['name'], "pack": item['pack'], "type": "未開封BOX" if "BOX" in item['name'].upper() else "シングルカード", "cond": cond, "qty": qty, "market_price": item['price'], "auto_update": True})
                                st.rerun()
        with tab_manual:
            man_name = st.text_input("商品名")
            man_pack = st.text_input("収録パック略号")
            c_type, c_cond = st.columns(2)
            with c_type: man_type = st.selectbox("種類", ["シングルカード", "未開封BOX", "未開封パック", "その他"])
            with c_cond: man_cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封", "-"])
            c_price, c_qty = st.columns(2)
            with c_price: man_price = st.number_input("参考相場", min_value=0, step=100)
            with c_qty: man_qty = st.number_input("数量", min_value=1, value=1)
            if st.button("✍️ 手動追加", use_container_width=True):
                if man_name:
                    st.session_state['cart'].append({"id": str(uuid.uuid4())[:8], "name": man_name, "pack": man_pack, "type": man_type, "cond": man_cond, "qty": man_qty, "market_price": man_price, "auto_update": False})
                    st.rerun()
        with tab_bulk:
            bulk_type = st.selectbox("素材の種類", ["【素材】SR", "【素材】AR", "【素材】RR", "【素材】CHR", "【素材】K", "【素材】汎用ノーマル"])
            bulk_qty = st.number_input("枚数", min_value=1, value=100)
            if st.button("素材追加"):
                st.session_state['cart'].append({"id": str(uuid.uuid4())[:8], "name": bulk_type, "pack": "", "type": "素材・バルク", "cond": "プレイ用", "qty": bulk_qty, "market_price": 30, "auto_update": False})
                st.rerun()
        with tab_supply:
            sup_name = st.text_input("サプライ品名")
            sup_qty = st.number_input("個数", min_value=1, value=1)
            if st.button("サプライ追加"):
                if sup_name:
                    st.session_state['cart'].append({"id": str(uuid.uuid4())[:8], "name": f"【サプライ】{sup_name}", "pack": "", "type": "サプライ", "cond": "-", "qty": sup_qty, "market_price": 0, "auto_update": False})
                    st.rerun()

    with col_right:
        st.subheader("② カートの中身と原価計算")
        c_save, c_load = st.columns(2)
        with c_save:
            if st.button("💾 下書き保存", use_container_width=True):
                save_cart_draft(st.session_state['session_id'], st.session_state['cart'])
                st.success("保存完了")
        with c_load:
            if st.button("📥 復元", use_container_width=True):
                draft = load_cart_draft(st.session_state['session_id'])
                if draft: st.session_state['cart'] = draft; st.rerun()

        rk = st.session_state['reset_key']
        with st.container(border=True):
            total_paid = st.number_input("支払総額", min_value=0, step=1000, key=f"total_paid_{rk}")
            purchase_title = st.text_input("仕入名目", key=f"title_{rk}")
            purchase_source = st.selectbox("仕入先", ["店舗", "フリマ", "オンラインオリパ", "問屋", "自己所有", "その他"], key=f"source_{rk}")
            is_individual = st.checkbox("✅ 個別管理する（細胞分裂）", value=True)
            
        if not st.session_state['cart']: st.caption("カートは空です")
        else:
            total_mkt = sum(item['qty'] * item['market_price'] for item in st.session_state['cart'])
            calc_cart = []
            for item in st.session_state['cart']:
                if 'unit_cost' not in item or total_paid != st.session_state.get('prev_total_paid', 0):
                    item_mkt = item['qty'] * item['market_price']
                    item['unit_cost'] = int((total_paid * (item_mkt / total_mkt)) / item['qty']) if total_mkt > 0 else 0
                calc_cart.append({"削除": False, "ID": item['id'], "商品名": item['name'], "収録パック": item.get('pack', ''), "状態": item['cond'], "種類": item['type'], "数量": item['qty'], "原価": item['unit_cost'], "参考相場": item['market_price']})
            st.session_state['prev_total_paid'] = total_paid
            
            edited_cart = st.data_editor(pd.DataFrame(calc_cart), hide_index=True, key=f"cart_ed_{rk}", use_container_width=True)
            
            if st.button("✨ 一括登録 ✨", type="primary", use_container_width=True):
                df_inv = load_data()
                batch_id, p_date = "B" + str(uuid.uuid4())[:7], datetime.now().strftime('%Y-%m-%d')
                new_rows, log_items = [], []
                for _, row in edited_cart.iterrows():
                    qty, cost = int(row['数量']), int(row['原価'])
                    log_items.append({'name': row['商品名'], 'pack': row['収録パック'], 'type': row['種類'], 'cond': row['状態'], 'qty': qty, 'unit_cost': cost, 'subtotal': qty * cost})
                    if row['種類'] == "サプライ": continue
                    if is_individual and row['種類'] not in ["素材・バルク", "その他"]:
                        for _ in range(qty):
                            new_rows.append({'ID': "P" + str(uuid.uuid4())[:7], '商品名': row['商品名'], '収録パック': row['収録パック'], '種類': row['種類'], '状態_PSA': row['状態'], '仕入日': p_date, '原価': cost, '参考相場': row['参考相場'], '在庫数': 1, '仕入元': purchase_source, 'ステータス': '在庫あり', 'PSA番号': '', '相場更新': True, '重量': '', '個別メモ': ''})
                    else:
                        mask = (df_inv['商品名'] == row['商品名']) & (df_inv['状態_PSA'] == row['状態']) & (df_inv['収録パック'] == row['収録パック'])
                        if not df_inv.empty and mask.any():
                            idx = df_inv[mask].index[0]
                            old_q, old_c = int(df_inv.at[idx, '在庫数']), int(df_inv.at[idx, '原価'])
                            df_inv.at[idx, '在庫数'] = old_q + qty
                            df_inv.at[idx, '原価'] = int((old_q * old_c + qty * cost) / (old_q + qty))
                            df_inv.at[idx, '仕入日'] = p_date
                        else:
                            new_rows.append({'ID': row['ID'], '商品名': row['商品名'], '収録パック': row['収録パック'], '種類': row['種類'], '状態_PSA': row['状態'], '仕入日': p_date, '原価': cost, '参考相場': row['参考相場'], '在庫数': qty, '仕入元': purchase_source, 'ステータス': '在庫あり', 'PSA番号': '', '相場更新': True, '重量': '', '個別メモ': ''})
                if new_rows: df_inv = pd.concat([df_inv, pd.DataFrame(new_rows)], ignore_index=True)
                save_data(df_inv)
                record_purchase_items(batch_id, p_date, purchase_title or "一括仕入", purchase_source, "カート登録", log_items)
                st.session_state['cart'] = []; st.session_state['reset_key'] += 1; st.success("登録完了"); time.sleep(1.5); st.rerun()

# =========================================================
# 📊 第2フェーズ：在庫・PSA管理
# =========================================================
elif menu == "📊 在庫・PSA管理":
    st.header("📊 在庫・PSA管理")
    df = load_data()
    if df.empty: st.info("在庫がありません")
    else:
        df_active = df[df['ステータス'] != '売却済み'].copy()
        tab_singles, tab_box, tab_psa, tab_sell, tab_edit, tab_maint = st.tabs(["🃏 シングル", "📦 BOX・素材", "💎 PSA管理", "🛒 売却レジ", "✏️ 編集", "🛠️ メンテ"])
        
        with tab_singles:
            df_s = df_active[(df_active['種類'] == 'シングルカード') & (~df_active['ステータス'].isin(['PSA提出中', '鑑定済み']))]
            st.dataframe(df_s[['商品名', '収録パック', '状態_PSA', '原価', '参考相場', '在庫数', '仕入日', '個別メモ']], hide_index=True, use_container_width=True)
            st.divider()
            target = st.selectbox("PSA提出するカードを選択", options=df_s['ID'].tolist(), format_func=lambda x: f"{df_s[df_s['ID']==x].iloc[0]['商品名']} (ID:{x})", index=None)
            if target and st.button("✈️ PSA提出中にする"):
                df.loc[df['ID'] == target, 'ステータス'] = 'PSA提出中'
                save_data(df); st.success("変更完了"); st.rerun()

        with tab_box:
            df_b = df_active[df_active['種類'].isin(['未開封BOX', '素材・バルク', 'オリジナルパック', '未開封パック'])]
            st.dataframe(df_b[['商品名', '種類', '原価', '在庫数', '参考相場', '重量', '個別メモ']], hide_index=True, use_container_width=True)

        with tab_psa:
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("##### ⏳ 提出中")
                st.dataframe(df_active[df_active['ステータス']=='PSA提出中'][['商品名', '在庫数', '原価']], hide_index=True)
            with c2:
                st.markdown("##### ✨ 鑑定済み")
                st.dataframe(df_active[df_active['ステータス']=='鑑定済み'][['商品名', '状態_PSA', 'PSA番号', '原価']], hide_index=True)
            st.divider()
            st.markdown("##### 📥 鑑定結果の登録")
            psa_p = df_active[df_active['ステータス']=='PSA提出中']
            if not psa_p.empty:
                with st.form("psa_res"):
                    tid = st.selectbox("カード選択", options=psa_p['ID'].tolist(), format_func=lambda x: f"{psa_p[psa_p['ID']==x].iloc[0]['商品名']} (ID:{x})")
                    cc1, cc2, cc3 = st.columns(3)
                    with cc1: gr = st.selectbox("鑑定結果", ["10", "9", "8", "7以下"])
                    with cc2: cert = st.text_input("PSA番号")
                    with cc3: fee = st.number_input("鑑定料", value=3300)
                    if st.form_submit_button("登録"):
                        df = load_data()
                        trow = df[df['ID'] == tid].iloc[0]
                        new_cost = int(trow['原価']) + fee
                        if int(trow['在庫数']) > 1:
                            df.loc[df['ID'] == tid, '在庫数'] = int(trow['在庫数']) - 1
                            new_r = trow.copy()
                            new_r['ID'], new_r['在庫数'], new_r['ステータス'], new_r['状態_PSA'], new_r['PSA番号'], new_r['原価'] = "I"+str(uuid.uuid4())[:7], 1, '鑑定済み', f"PSA {gr}", cert, new_cost
                            df = pd.concat([df, pd.DataFrame([new_r])], ignore_index=True)
                        else:
                            df.loc[df['ID'] == tid, 'ステータス'], df.loc[df['ID'] == tid, '状態_PSA'], df.loc[df['ID'] == tid, 'PSA番号'], df.loc[df['ID'] == tid, '原価'] = '鑑定済み', f"PSA {gr}", cert, new_cost
                        save_data(df)
                        df_s = load_sales_data()
                        df_s = pd.concat([df_s, pd.DataFrame([{'ID': "S"+str(uuid.uuid4())[:7], '元の在庫ID': tid, '売却日': datetime.now().strftime('%Y-%m-%d'), '商品名': trow['商品名'], '収録パック': trow['収録パック'], '状態_PSA': trow['状態_PSA'], '売却数': 1, '売上額': 0, '手数料': 0, '経費_送料': 0, '純利益': 0, '販路': 'システム：PSA移行', '備考': 'PSA登録による自動処理', '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}])], ignore_index=True)
                        save_sales_data(df_s)
                        st.success("登録完了"); time.sleep(1); st.rerun()

        with tab_sell:
            st.subheader("🛒 売却レジ (まとめ買いスキャン対応)")
            c_left, c_right = st.columns([1.2, 1])
            
            with c_left:
                scan_mode = st.radio("スキャン方法", ["🔫 物理スキャナー", "📱 スマホ内蔵カメラ"], horizontal=True)
                target_sell_id = None
                active_ids = {f"[{r['収録パック']}] {r['商品名']} ({r['状態_PSA']} | 残:{r['在庫数']}) [ID:{r['ID']}]": r['ID'] for _, r in df_active[df_active['在庫数'] > 0].iterrows()}
                existing_cart_ids = [item['id'] for item in st.session_state['sell_cart']]
                
                if scan_mode == "🔫 物理スキャナー":
                    st.text_input("📷 スキャンしてカートに追加", key="phys_scan_val_sell", on_change=cb_phys_sell)
                    target_sell_id = st.session_state.pop('phys_scan_pend_sell', None)
                else:
                    st.info("カメラへのアクセスを許可し、枠内にQRコードを写してください。音が出ます。")
                    cam_res = _scanner(scanned_ids=existing_cart_ids, valid_ids=list(active_ids.values()), key="cam_sell")
                    if cam_res and isinstance(cam_res, dict):
                        if cam_res['ts'] != st.session_state.get('last_cam_ts_sell'):
                            st.session_state['last_cam_ts_sell'] = cam_res['ts']
                            target_sell_id = cam_res['id']

                manual_sell = st.selectbox("手動で選んで追加", options=[""] + list(active_ids.keys()), index=0)
                if manual_sell != "": target_sell_id = active_ids[manual_sell]

                if target_sell_id:
                    if target_sell_id in active_ids.values():
                        if target_sell_id not in existing_cart_ids:
                            trow = df_active[df_active['ID'] == target_sell_id].iloc[0]
                            def_price = int(trow['参考相場']) if int(trow['参考相場']) > 0 else int(trow['原価'])
                            st.session_state['sell_cart'].append({
                                '削除': False, 'id': target_sell_id, 'name': trow['商品名'], 'pack': trow['収録パック'], 
                                'cond': trow['状態_PSA'], 'cost': int(trow['原価']), 
                                'sell_price': def_price, 'qty': 1, 'max_qty': int(trow['在庫数'])
                            })
                            st.toast(f"✅ レジに追加しました: {trow['商品名']}", icon="🛒")
                        else:
                            st.toast("⚠️ すでにレジに入っています。数量を変更してください。", icon="⚠️")
                    else:
                        st.toast("❌ 在庫が見つかりません。", icon="❌")
                    st.rerun()

                st.write("---")
                if not st.session_state['sell_cart']:
                    st.info("商品をスキャンするか、リストから選んでレジに追加してください。")
                else:
                    st.markdown("#### 🛍️ お会計カート")
                    df_sell_cart = pd.DataFrame(st.session_state['sell_cart'])
                    edited_sell = st.data_editor(
                        df_sell_cart[['削除', 'name', 'cond', 'sell_price', 'qty', 'id']],
                        hide_index=True,
                        column_config={
                            "削除": st.column_config.CheckboxColumn("外す", default=False, width="small"),
                            "name": st.column_config.TextColumn("商品名", disabled=True),
                            "cond": st.column_config.TextColumn("状態", disabled=True, width="small"),
                            "sell_price": st.column_config.NumberColumn("売値(手入力可)", min_value=0, step=100, format="¥%d"),
                            "qty": st.column_config.NumberColumn("売却数", min_value=1, step=1, width="small"),
                            "id": None
                        }, use_container_width=True
                    )
                    
                    needs_rerun = False
                    for idx, row in edited_sell.iterrows():
                        for item in st.session_state['sell_cart']:
                            if item['id'] == row['id']:
                                act_qty = row['qty'] if row['qty'] <= item['max_qty'] else item['max_qty']
                                if item['sell_price'] != row['sell_price'] or item['qty'] != act_qty:
                                    item['sell_price'] = row['sell_price']
                                    item['qty'] = act_qty
                                    needs_rerun = True
                    if needs_rerun: st.rerun()
                    
                    if edited_sell['削除'].any():
                        if st.button("🗑️ チェックした商品を外す"):
                            keep_ids = edited_sell[~edited_sell['削除']]['id'].tolist()
                            st.session_state['sell_cart'] = [i for i in st.session_state['sell_cart'] if i['id'] in keep_ids]
                            st.rerun()

            with c_right:
                # ✨ v5.7 売却レジ用クリアボタンを配置
                if st.button("🗑️ カートとスキャン履歴をクリア", use_container_width=True):
                    st.session_state['sell_cart'] = []
                    st.rerun()
                
                with st.container(border=True):
                    if not st.session_state['sell_cart']:
                        st.write("カートは空です")
                    else:
                        total_sales = sum(item['sell_price'] * item['qty'] for item in st.session_state['sell_cart'])
                        total_cost = sum(item['cost'] * item['qty'] for item in st.session_state['sell_cart'])
                        
                        st.markdown(f"### 💰 売上合計: ¥{total_sales:,}")
                        st.caption(f"原価合計: ¥{total_cost:,}")
                        
                        ch = st.selectbox("販路", ["BASE (Web)", "BASE (PayID)", "メルカリ", "店舗・直接", "その他"])
                        sc = st.number_input("送料・梱包費 (全体)", min_value=0, value=185 if "店舗" not in ch else 0)
                        note = st.text_input("全体メモ (レシート共通)")
                        
                        if st.button("✨ 一括で会計を確定 ✨", type="primary", use_container_width=True):
                            df_inv_s = load_data()
                            df_sales_s = load_sales_data()
                            receipt_id = "R" + str(uuid.uuid4())[:7]
                            sales_records = []
                            
                            for item in st.session_state['sell_cart']:
                                s_price = item['sell_price'] * item['qty']
                                fee = int(s_price * 0.066 + 40) if "Web" in ch else int(s_price * 0.095 + 40) if "PayID" in ch else int(s_price * 0.1) if "メルカリ" in ch else 0
                                prorated_sc = int(sc * (s_price / total_sales)) if total_sales > 0 else int(sc / len(st.session_state['sell_cart']))
                                profit = s_price - fee - prorated_sc - (item['cost'] * item['qty'])
                                
                                new_q = int(df_inv_s.loc[df_inv_s['ID'] == item['id'], '在庫数'].values[0]) - item['qty']
                                df_inv_s.loc[df_inv_s['ID'] == item['id'], '在庫数'] = new_q
                                if new_q <= 0: df_inv_s.loc[df_inv_s['ID'] == item['id'], 'ステータス'] = '売却済み'
                                
                                sales_records.append({
                                    'ID': "S"+str(uuid.uuid4())[:7], '元の在庫ID': item['id'], '売却日': datetime.now().strftime('%Y-%m-%d'), 
                                    '商品名': item['name'], '収録パック': item['pack'], '状態_PSA': item['cond'], 
                                    '売却数': item['qty'], '売上額': s_price, '手数料': fee, '経費_送料': prorated_sc, 
                                    '純利益': profit, '販路': ch, '備考': f"{note} [明細:{receipt_id}]".strip(), 
                                    '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                })
                                
                            save_data(df_inv_s)
                            save_sales_data(pd.concat([df_sales_s, pd.DataFrame(sales_records)], ignore_index=True))
                            
                            st.session_state['sell_cart'] = []
                            st.success(f"🎉 お会計完了！ (レシート番号: {receipt_id})"); time.sleep(2); st.rerun()

        with tab_edit:
            df_edit = df.copy(); df_edit['削除'] = False
            ed = st.data_editor(df_edit[['削除', '商品名', '収録パック', '種類', '状態_PSA', '重量', '個別メモ', '在庫数', '原価', 'ステータス', 'ID']], hide_index=True, use_container_width=True)
            if st.button("💾 変更を保存", type="primary"):
                df_s = load_data()
                keep_ids = ed[~ed['削除']]['ID'].tolist()
                df_s = df_s[df_s['ID'].isin(keep_ids)].copy()
                for _, r in ed.iterrows():
                    if not r['削除']:
                        for col in ['商品名', '収録パック', '状態_PSA', '重量', '個別メモ', '在庫数', '原価', 'ステータス']: df_s.loc[df_s['ID'] == r['ID'], col] = r[col]
                save_data(df_s); st.success("更新完了"); st.rerun()

        with tab_maint:
            st.subheader("🛠️ メンテナンス")
            with st.container(border=True):
                st.markdown("#### ✂️ 在庫の個別化（細胞分裂）")
                df_to_split = df_active[df_active['在庫数'] > 1].copy()
                if df_to_split.empty: st.info("個別化できるまとめ在庫はありません。")
                else:
                    df_to_split['分割対象'] = False
                    split_ed = st.data_editor(df_to_split[['分割対象', '商品名', '収録パック', '状態_PSA', '在庫数', 'ID']], hide_index=True, use_container_width=True)
                    selected_split = split_ed[split_ed['分割対象'] == True]
                    if not selected_split.empty and st.button("🚨 選択した在庫を1点ずつにバラバラにする", type="primary", use_container_width=True):
                        df_main = load_data()
                        for _, s_row in selected_split.iterrows():
                            tid, qty = s_row['ID'], int(s_row['在庫数'])
                            orig_row = df_main[df_main['ID'] == tid].iloc[0]
                            df_main = df_main[df_main['ID'] != tid]
                            for _ in range(qty):
                                new_r = orig_row.copy()
                                new_r['ID'], new_r['在庫数'] = "P" + str(uuid.uuid4())[:7], 1
                                df_main = pd.concat([df_main, pd.DataFrame([new_r])], ignore_index=True)
                        save_data(df_main); st.success("細胞分裂が完了しました！"); time.sleep(2); st.rerun()

            with st.container(border=True):
                st.markdown("#### 🔗 在庫おまとめ（商品統合）")
                df_to_m = df_active.copy(); df_to_m['統合対象'] = False
                m_search = st.text_input("🔍 統合検索")
                if m_search: df_to_m = df_to_m[df_to_m['商品名'].str.contains(m_search, na=False)]
                m_ed = st.data_editor(df_to_m[['統合対象', '商品名', '状態_PSA', '在庫数', '原価', 'ID']], hide_index=True, use_container_width=True)
                sel_m = m_ed[m_ed['統合対象'] == True]
                if len(sel_m) >= 2:
                    if st.button("🚨 統合確定"):
                        df_m = load_data()
                        t_qty = sel_m['在庫数'].sum(); t_cost = int((sel_m['原価'] * sel_m['在庫数']).sum() / t_qty)
                        master_id = sel_m.iloc[0]['ID']; master_r = df_active[df_active['ID'] == master_id].iloc[0].copy()
                        df_m = df_m[~df_m['ID'].isin(sel_m['ID'].tolist())]
                        master_r['在庫数'], master_r['原価'] = t_qty, t_cost
                        df_m = pd.concat([df_m, pd.DataFrame([master_r])], ignore_index=True)
                        save_data(df_m); st.success("統合完了"); st.rerun()

            st.button("🚨 原価を全再計算する (神の計算機)", on_click=lambda: save_data(recalculate_moving_average_costs()))

# =========================================================
# 🖨️ 個別管理・ラベル
# =========================================================
elif menu == "🖨️ 個別管理・ラベル":
    st.header("🖨️ 個別管理・A4ラベル印刷")
    df = load_data()
    if not df.empty:
        df_act = df[(df['ステータス'] == '在庫あり') & (df['在庫数'] == 1)].copy()
        s_lbl = st.text_input("🔍 商品名検索")
        if s_lbl: df_act = df_act[df_act['商品名'].str.contains(s_lbl, na=False)]
        if df_act.empty: st.info("個別管理対象がありません")
        else:
            df_act['印刷対象'] = False
            st.markdown("##### 📝 1. 情報の編集と印刷対象の選択")
            l_ed = st.data_editor(
                df_act[['印刷対象', '商品名', '状態_PSA', '重量', '個別メモ', 'ID']], 
                hide_index=True, 
                column_config={
                    "印刷対象": st.column_config.CheckboxColumn("印刷", default=False, width="small"),
                    "商品名": st.column_config.TextColumn("商品名", disabled=True),
                    "状態_PSA": st.column_config.TextColumn("状態", disabled=True, width="small"),
                    "重量": st.column_config.TextColumn("重量(g)"),
                    "個別メモ": st.column_config.TextColumn("ラベル印字メモ (2行程度まで)"),
                    "ID": None
                }, use_container_width=True)
                
            if st.button("💾 重量・メモを保存", type="primary"):
                df_s = load_data()
                for _, r in l_ed.iterrows():
                    df_s.loc[df_s['ID'] == r['ID'], '重量'] = r['重量']
                    df_s.loc[df_s['ID'] == r['ID'], '個別メモ'] = r['個別メモ']
                save_data(df_s); st.success("保存完了"); st.rerun()
                
            sel_p = l_ed[l_ed['印刷対象'] == True]
            st.divider()
            st.markdown("##### 🖨️ 2. ラベル用紙への印刷 (A4・24面)")
            st.caption("※ショップ名を削除し、メモが印字される新デザインです。")
            
            start_pos = st.number_input("📌 印刷開始位置 (1〜24)", min_value=1, max_value=24, value=1, help="使いかけのシール用紙を使う場合、何番目のシールから印刷を始めるかを指定します。")
            
            if not sel_p.empty:
                items = [df_act[df_act['ID'] == r['ID']].iloc[0].to_dict() for _, r in sel_p.iterrows()]
                st.download_button(f"📄 {len(items)}枚のラベルHTMLをダウンロード", generate_label_html(items, start_pos=start_pos), file_name=f"labels_start{start_pos}.html", mime="text/html", type="primary")
            else:
                st.button("📄 ラベルHTMLをダウンロード", disabled=True)

# =========================================================
# 🛍️ オリパ工場
# =========================================================
elif menu == "🛍️ オリパ工場":
    st.header("🛍️ オリパ工場")
    df = load_data()
    if not df.empty:
        df_av = df[(df['ステータス'] == '在庫あり') | (df['ステータス'] == '鑑定済み')].copy()
        
        col_l, col_r = st.columns([1.5, 1])
        with col_l:
            st.subheader("① 封入するカード・素材の選択")
            
            scan_mode = st.radio("素材の追加方法", ["🔫 物理スキャナー", "📱 スマホ内蔵カメラ (連続可)"], horizontal=True)
            scan_oripa = None
            
            if scan_mode == "🔫 物理スキャナー":
                st.text_input("📷 スキャンして追加 (ID入力)", key="phys_scan_val_oripa", on_change=cb_phys_oripa)
                scan_oripa = st.session_state.pop('phys_scan_pend_oripa', None)
            else:
                st.info("カメラへのアクセスを許可し、シールのQRコードをかざしてください。音が出ます。")
                cam_res = _scanner(scanned_ids=st.session_state['oripa_scanned'], valid_ids=list(df_av['ID'].values), key="cam_oripa")
                if cam_res and isinstance(cam_res, dict):
                    if cam_res['ts'] != st.session_state.get('last_cam_ts_oripa'):
                        st.session_state['last_cam_ts_oripa'] = cam_res['ts']
                        scan_oripa = cam_res['id']

            if scan_oripa:
                if scan_oripa in df_av['ID'].values:
                    if scan_oripa not in st.session_state['oripa_scanned']:
                        st.session_state['oripa_scanned'].append(scan_oripa)
                        added_item_name = df_av[df_av['ID'] == scan_oripa].iloc[0]['商品名']
                        st.toast(f"✅ スキャン完了: {added_item_name} を追加しました！", icon="🎉")
                    else:
                        st.toast(f"⚠️ すでに追加されています: {scan_oripa}", icon="⚠️")
                else:
                    st.toast(f"❌ 在庫が見つかりません: {scan_oripa}", icon="❌")
                st.rerun()

            if st.session_state['oripa_scanned']:
                with st.container(border=True):
                    st.markdown("#### 📥 今回の封入リスト（スキャン済）")
                    scanned_items = df_av[df_av['ID'].isin(st.session_state['oripa_scanned'])]
                    if not scanned_items.empty:
                        st.dataframe(
                            scanned_items[['商品名', '状態_PSA', '原価', 'ID', '個別メモ']], 
                            hide_index=True, 
                            use_container_width=True
                        )
                st.divider()

            st.markdown("##### 📦 全在庫リスト (手動選択も可能)")
            df_av['オリパに使う'] = False; df_av['使用数'] = 0
            for s in st.session_state['oripa_scanned']:
                if s in df_av['ID'].values: df_av.loc[df_av['ID'] == s, 'オリパに使う'], df_av.loc[df_av['ID'] == s, '使用数'] = True, 1
            o_ed = st.data_editor(df_av[['オリパに使う', '商品名', '原価', '在庫数', '使用数', 'ID', '個別メモ']], hide_index=True, use_container_width=True)
            sel_o = o_ed[(o_ed['オリパに使う'] == True) & (o_ed['使用数'] > 0)]
            
        with col_r:
            if st.button("🗑️ スキャン履歴クリア"):
                st.session_state['oripa_scanned'] = []
                st.rerun()
                
            o_name = st.text_input("オリパ名称")
            total_u = st.number_input("全口数", min_value=1, value=100)
            u_price = st.number_input("販売単価", min_value=0, value=1000)
            s_fee = st.number_input("送料/口", value=185); p_fee = st.number_input("梱包/口", value=50)
            
            if not sel_o.empty:
                m_cost = sum(sel_o['原価'] * sel_o['使用数']); e_cost = (s_fee + p_fee) * total_u
                t_cost = m_cost + e_cost; u_cost = int(t_cost / total_u)
                st.metric("総原価", f"¥{t_cost:,}"); st.metric("見込み純利益", f"¥{(u_price * total_u) - t_cost:,}")
                if o_name and st.button("🔨 オリパ作成", type="primary", use_container_width=True):
                    df = load_data()
                    s_recs = []
                    for _, row in sel_o.iterrows():
                        df.loc[df['ID'] == row['ID'], '在庫数'] -= int(row['使用数'])
                        if df.loc[df['ID'] == row['ID'], '在庫数'].values[0] <= 0: df.loc[df['ID'] == row['ID'], 'ステータス'] = 'オリパ消費'
                        s_recs.append({'ID': "S"+str(uuid.uuid4())[:7], '元の在庫ID': row['ID'], '売却日': datetime.now().strftime('%Y-%m-%d'), '商品名': row['商品名'], '収録パック': '', '状態_PSA': '-', '売却数': row['使用数'], '売上額': 0, '手数料': 0, '経費_送料': 0, '純利益': 0, '販路': 'システム：オリパ消費', '備考': f'オリパ[{o_name}]素材', '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                    df = pd.concat([df, pd.DataFrame([{'ID': "O"+str(uuid.uuid4())[:7], '商品名': f"【オリパ】{o_name}", '種類': 'オリジナルパック', '在庫数': total_u, '原価': u_cost, '参考相場': u_price, 'ステータス': '在庫あり', '仕入日': datetime.now().strftime('%Y-%m-%d'), '相場更新': False, '重量': '', '個別メモ': ''}])], ignore_index=True)
                    save_data(df); save_sales_data(pd.concat([load_sales_data(), pd.DataFrame(s_recs)], ignore_index=True))
                    st.session_state['oripa_scanned'] = []
                    st.success("作成完了"); st.rerun()

# =========================================================
# 📖 帳簿・分析
# =========================================================
elif menu == "📖 帳簿・分析":
    st.header("📖 帳簿・分析")
    df_inv, df_pur, df_sales = load_data(), load_purchase_data(), load_sales_data()
    t1, t2, t3, t4 = st.tabs(["📈 状況", "📒 売上", "📒 仕入", "📤 出力"])
    with t1:
        if not df_inv.empty:
            df_act = df_inv[df_inv['ステータス'] != '売却済み']
            c1, c2 = st.columns(2); c1.metric("在庫原価総額", f"¥{(df_act['原価']*df_act['在庫数']).sum():,}"); c2.metric("見込み売上", f"¥{(df_act['参考相場']*df_act['在庫数']).sum():,}")
    with t2: st.dataframe(df_sales, hide_index=True)
    with t3: st.dataframe(df_pur, hide_index=True)
    with t4:
        st.download_button("📤 在庫CSV", df_inv.to_csv(index=False).encode('utf-8-sig'), "inventory.csv", "text/csv")
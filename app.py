import streamlit as st
import pandas as pd
import os
import uuid
import requests
import re
import time
import json
import difflib
import traceback
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe
import streamlit.components.v1 as components

# ---------------------------------------------------------
# ⚙️ 設定・定数 (v5.56 - 列競合解消・完全固定版)
# ---------------------------------------------------------
JSON_KEY_FILE = 'secrets.json'
SPREADSHEET_NAME = 'ぽっけぇ〜道_システムv3'

SHEET_INVENTORY = '在庫DB'
SHEET_PURCHASE = '仕入帳'
SHEET_SALES = '売上帳'
SHEET_CART = 'カート下書き'
SHEET_SETTINGS = 'システム設定'

UPDATE_BATCH_SIZE = 10 

# ---------------------------------------------------------
# 🔔 Discord通知用エンジン
# ---------------------------------------------------------
def send_discord_alert(message, is_test=False):
    try:
        webhook_url = st.secrets.get("DISCORD_WEBHOOK_URL")
        if not webhook_url:
            if is_test: st.error("❌ 金庫(Secrets)の中に 'DISCORD_WEBHOOK_URL' が見つかりません。")
            return False
        data = {"content": message}
        res = requests.post(webhook_url, json=data, timeout=5)
        if res.status_code in [200, 204]:
            if is_test: st.success("✅ Discordへの送信に成功しました！")
            return True
        else:
            if is_test: st.error(f"❌ Discordが拒否しました (エラーコード: {res.status_code})。")
            return False
    except Exception as e:
        if is_test: st.error(f"❌ 通信エラー: {e}")
        return False

# ---------------------------------------------------------
# 🧪 スニダン(SNKRDUNK) 取得実験用エンジン
# ---------------------------------------------------------
def test_snkrdunk_scraping(keyword):
    encoded = quote(keyword.encode('utf-8'))
    url = f"https://snkrdunk.com/search/result?keyword={encoded}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = "utf-8"
        status = res.status_code
        html_text = res.text
        is_blocked = status in [403, 401, 429] or "cloudflare" in html_text.lower() or "just a moment" in html_text.lower()
        return {"url": url, "status": status, "is_blocked": is_blocked, "html_snippet": html_text[:500]}
    except Exception as e:
        return {"status": "Exception", "is_blocked": False, "message": str(e)}

# ---------------------------------------------------------
# 📷 スマホ内蔵カメラ用 QRスキャナー部品
# ---------------------------------------------------------
QR_HTML = """
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><script src="https://unpkg.com/html5-qrcode" type="text/javascript"></script></head>
<body style="margin:0; padding:5px; font-family:sans-serif; background:#f0f2f6; display:flex; flex-direction:column;">
  <div id="audio-unlock" style="text-align:center; padding:12px; background:#e0f7fa; color:#3182ce; font-weight:bold; cursor:pointer; border-radius:8px; margin-bottom:10px;">🔊 タップして通知音をON</div>
  <div id="reader" style="width:100%; max-width:500px; margin:0 auto; border-radius:8px; overflow:hidden; border:1px solid #ddd; background:#fff; min-height:350px;"></div>
  <script>
    let scannedIds = []; let validIds = [];
    const AudioContext = window.AudioContext || window.webkitAudioContext; const ctx = new AudioContext();
    document.getElementById('audio-unlock').addEventListener('click', function() {
        if (ctx.state === 'suspended') ctx.resume(); playBeep('success'); this.style.display = 'none'; updateHeight();
    });
    function playBeep(type) {
        if (ctx.state === 'suspended') return;
        const osc = ctx.createOscillator(); const gain = ctx.createGain();
        osc.connect(gain); gain.connect(ctx.destination);
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
    function sendValue(val) { window.parent.postMessage({ isStreamlitMessage: true, type: "streamlit:setComponentValue", value: {id: val, ts: Date.now()} }, "*"); }
    function updateHeight() {
      let h = document.body.scrollHeight + 20; if (h < 450) h = 450;
      window.parent.postMessage({ isStreamlitMessage: true, type: "streamlit:setFrameHeight", height: h }, "*");
    }
    function onDataFromPython(event) {
        if (event.data.type !== "streamlit:render") return;
        if (event.data.args.scanned_ids) scannedIds = event.data.args.scanned_ids;
        if (event.data.args.valid_ids) validIds = event.data.args.valid_ids; updateHeight();
    }
    window.addEventListener("message", onDataFromPython);
    window.onload = function() {
      window.parent.postMessage({ isStreamlitMessage: true, type: "streamlit:componentReady", apiVersion: 1 }, "*");
      let lastScanned = "";
      let scanner = new Html5QrcodeScanner("reader", { fps: 10, qrbox: {width: 250, height: 250}, aspectRatio: 1.0 }, false);
      scanner.render(function(txt) {
        if (txt !== lastScanned) {
            lastScanned = txt;
            if (validIds.length > 0 && !validIds.includes(txt)) playBeep('error');
            else if (scannedIds.includes(txt)) playBeep('error');
            else playBeep('success');
            sendValue(txt); setTimeout(() => { lastScanned = ""; }, 1500);
        }
      });
      setTimeout(updateHeight, 500);
    };
  </script>
</body></html>
"""

def get_camera_qr_scanner():
    comp_dir = os.path.join(os.path.dirname(__file__), "qr_cam_comp")
    os.makedirs(comp_dir, exist_ok=True)
    idx_path = os.path.join(comp_dir, "index.html")
    with open(idx_path, "w", encoding="utf-8") as f: f.write(QR_HTML)
    return components.declare_component("camera_qr_scanner", path=comp_dir)

_scanner = get_camera_qr_scanner()

# ---------------------------------------------------------
# 🛡️ Google API接続エンジン
# ---------------------------------------------------------
@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        if "gcp_service_account" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
        elif "private_key" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets, scope)
        elif os.path.exists(JSON_KEY_FILE):
            creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
        else:
            st.error(f"❌ 認証エラー: {JSON_KEY_FILE} または Streamlit Secrets にキーが見つかりません。")
            return None
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ 認証エラー (Google API): {e}")
        st.code(traceback.format_exc())
        return None

def get_spreadsheet():
    client = get_gspread_client()
    if not client: return None
    try:
        return client.open(SPREADSHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ エラー: スプレッドシート '{SPREADSHEET_NAME}' が見つかりません。名前が正しいか、権限があるか確認してください。")
        return None
    except Exception as e:
        st.error(f"❌ スプレッドシート取得エラー: {e}")
        st.code(traceback.format_exc())
        return None

def check_and_init_sheets():
    sh = get_spreadsheet()
    if not sh: return None, None, None, None, None
    try:
        worksheets = sh.worksheets()
        sheets = {ws.title: ws for ws in worksheets}
        
        def get_or_create(title, rows, cols, headers):
            if title in sheets: return sheets[title]
            ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
            ws.append_row(headers)
            return ws
            
        # 💡 列構成を18列に完全固定
        ws_inv = get_or_create(SHEET_INVENTORY, 1000, 18, ['ID', '商品名', '収録パック', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号', '相場更新', '重量', '個別メモ', '商品URL', 'スニダンURL', 'BASE販売価格'])
        ws_pur = get_or_create(SHEET_PURCHASE, 1000, 14, ['ID', '仕入日', '仕入名目', '商品名', '収録パック', '種類', '状態_PSA', '数量', '単価', '小計', '仕入先', '備考', '登録日時'])
        ws_sales = get_or_create(SHEET_SALES, 1000, 15, ['ID', '元の在庫ID', '売却日', '商品名', '収録パック', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時'])
        ws_cart = get_or_create(SHEET_CART, 1000, 3, ['SessionID', 'Timestamp', 'CartJSON'])
        ws_set = get_or_create(SHEET_SETTINGS, 50, 2, ['Key', 'Value'])
        
        return ws_inv, ws_pur, ws_sales, ws_cart, ws_set
    except Exception as e:
        st.error(f"❌ シート初期化エラー: {e}")
        st.code(traceback.format_exc())
        return None, None, None, None, None

def load_system_settings():
    _, _, _, _, ws_set = check_and_init_sheets()
    if not ws_set: return {}
    try:
        records = ws_set.get_all_records()
        return {str(row['Key']): str(row['Value']) for row in records}
    except Exception as e:
        st.error(f"❌ 設定読み込みエラー: {e}")
        return {}

def save_system_setting(key, value):
    _, _, _, _, ws_set = check_and_init_sheets()
    if not ws_set: return
    try:
        cell = ws_set.find(key, in_column=1)
        ws_set.update_cell(cell.row, 2, value)
    except gspread.exceptions.CellNotFound:
        ws_set.append_row([key, value])
    except Exception as e:
        st.error(f"❌ 設定保存エラー: {e}")

def get_base_items(access_token):
    url = "https://api.thebase.in/1/items"
    headers = {"Authorization": f"Bearer {access_token}"}
    items, offset = [], 0
    while True:
        try:
            res = requests.get(url, headers=headers, params={"limit": 100, "offset": offset}, timeout=10)
            if res.status_code != 200: break
            data = res.json()
            fetched = data.get('items', [])
            items.extend(fetched)
            if len(fetched) < 100: break
            offset += 100
        except Exception as e:
            st.error(f"❌ BASE API通信エラー: {e}")
            break
    return items

def clean_display_data(df, columns):
    if df is None or df.empty: return df
    for c in columns:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: str(x)[:-2] if str(x).endswith('.0') else str(x))
            df[c] = df[c].replace({'nan': '', 'None': '', 'NaN': '', '<NA>': ''})
    return df

# ---------------------------------------------------------
# 🚨 データ読み込みエンジン
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def load_data():
    ws_inv, _, _, _, _ = check_and_init_sheets()
    if not ws_inv:
        if st.button("🔄 画面をリロードして再試行", key="reload_inv_1"): st.rerun()
        return None
    try:
        header = ws_inv.row_values(1)
        if not header: return None
        # 💡 列設定
        required_cols = ['重量', '個別メモ', '商品URL', 'スニダンURL', 'BASE販売価格']
        updates = []
        for col in required_cols:
            if col not in header:
                header.append(col)
                updates.append(gspread.Cell(row=1, col=len(header), value=col))
        if updates: ws_inv.update_cells(updates)
        
        df = get_as_dataframe(ws_inv, evaluate_formulas=True)
        if 'ID' not in df.columns:
            st.error("❌ 在庫DBのA列に『ID』という見出しが見つかりません。")
            return None
            
        df = df.dropna(subset=['ID']); df = df[df['ID'] != '']
        df = clean_display_data(df, ['PSA番号', '収録パック', '重量', '個別メモ', '商品URL', 'スニダンURL', 'BASE販売価格', '仕入元', 'ステータス', '状態_PSA'])

        if '相場更新' not in df.columns: df['相場更新'] = True
        else:
            df['相場更新'] = df['相場更新'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False, '1': True, '0': False})
            df['相場更新'] = df['相場更新'].fillna(True).astype(bool)
        for c in ['原価', '参考相場', '在庫数']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"❌ 在庫データ読み込みエラー: {e}")
        st.code(traceback.format_exc())
        if st.button("🔄 画面をリロードして再試行", key="reload_inv_2"): st.rerun()
        return None

@st.cache_data(ttl=60)
def load_sales_data():
    _, _, ws_sales, _, _ = check_and_init_sheets()
    if not ws_sales: return None
    try:
        df = get_as_dataframe(ws_sales, evaluate_formulas=True)
        if df.empty or 'ID' not in df.columns: return pd.DataFrame()
        df = df.dropna(subset=['ID']); df = df[df['ID'] != '']
        df = clean_display_data(df, ['元の在庫ID', '収録パック', '状態_PSA', '販路', '備考'])
        for col in ['売却数', '売上額', '手数料', '経費_送料', '純利益']: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"❌ 売上帳エラー: {e}"); st.code(traceback.format_exc())
        if st.button("🔄 画面をリロード", key="reload_sale"): st.rerun()
        return None

@st.cache_data(ttl=60)
def load_purchase_data():
    _, ws_pur, _, _, _ = check_and_init_sheets()
    if not ws_pur: return None
    try:
        df = get_as_dataframe(ws_pur, evaluate_formulas=True)
        if df.empty or 'ID' not in df.columns: return pd.DataFrame()
        df = df.dropna(subset=['ID']); df = df[df['ID'] != '']
        df = clean_display_data(df, ['収録パック', '種類', '状態_PSA', '仕入先', '備考', '仕入名目'])
        for col in ['数量', '単価', '小計']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"❌ 仕入帳エラー: {e}"); st.code(traceback.format_exc())
        if st.button("🔄 画面をリロード", key="reload_pur"): st.rerun()
        return None

def generic_save(df=None, sheet_type=None, save_cols=None, default_values=None, is_append_mode=False, append_data=None):
    if df is None and not is_append_mode: return None
    ws_inv, ws_pur, ws_sales, _, _ = check_and_init_sheets()
    if sheet_type == 'inventory': ws, cache_clear = ws_inv, load_data.clear
    elif sheet_type == 'purchase': ws, cache_clear = ws_pur, load_purchase_data.clear
    elif sheet_type == 'sales': ws, cache_clear = ws_sales, load_sales_data.clear
    else: return df
    if not ws: return df
    
    try:
        if is_append_mode and append_data is not None:
            ws.append_rows(append_data)
            cache_clear(); return True
            
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns: df_to_save[col] = default_values[col] if (default_values and col in default_values) else ""
        df_to_save = df_to_save[save_cols]
        
        df_ex = get_as_dataframe(ws, evaluate_formulas=False).dropna(how='all')
        if df_ex.empty: df_ex = pd.DataFrame(columns=save_cols); df_ex['__row'] = pd.Series(dtype=int)
        else: df_ex['__row'] = df_ex.index + 2
        df_ex = df_ex.dropna(subset=['ID']); df_ex = df_ex[df_ex['ID'] != '']
        ex_cols = [c for c in save_cols if c in df_ex.columns]
        df_ex = df_ex[['ID', '__row'] + [c for c in ex_cols if c != 'ID']]
        
        merged = pd.merge(df_ex, df_to_save, on='ID', how='outer', suffixes=('_old', ''), indicator=True)
        cells_to_update = []
        next_new_row = int(df_ex['__row'].max()) + 1 if (not df_ex.empty and pd.notna(df_ex['__row'].max())) else 2
        
        for _, row in merged.iterrows():
            status = row['_merge']
            if status == 'both': 
                r = int(row['__row'])
                for c_idx, col in enumerate(save_cols):
                    old_val, new_val = row.get(f"{col}_old", None), row[col]
                    s_old, s_new = "" if pd.isna(old_val) else str(old_val).strip(), "" if pd.isna(new_val) else str(new_val).strip()
                    if s_old == s_new: continue
                    try:
                        if float(s_old.replace(',', '')) == float(s_new.replace(',', '')): continue
                    except ValueError: pass
                    if s_old.upper() == s_new.upper() and s_new.upper() in ['TRUE', 'FALSE']: continue
                    cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value="" if pd.isna(new_val) else new_val))
                    
            elif status == 'right_only': 
                r = next_new_row
                for c_idx, col in enumerate(save_cols):
                    new_val = row[col]
                    cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value="" if pd.isna(new_val) else new_val))
                next_new_row += 1 
                
        if cells_to_update: ws.update_cells(cells_to_update)
        cache_clear(); return df_to_save
        
    except Exception as e:
        st.error(f"❌ データ保存エラー: {e}")
        st.code(traceback.format_exc())
        return df

def save_data(df):
    if df is None: return None
    # 💡 保存対象を18列に設定
    save_cols = ['ID', '商品名', '収録パック', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号', '相場更新', '重量', '個別メモ', '商品URL', 'スニダンURL', 'BASE販売価格']
    return generic_save(df=df, sheet_type='inventory', save_cols=save_cols, default_values={'相場更新': True})

def save_sales_data(df):
    if df is None: return None
    save_cols = ['ID', '元の在庫ID', '売却日', '商品名', '収録パック', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時']
    return generic_save(df=df, sheet_type='sales', save_cols=save_cols)

def save_purchase_data(df):
    if df is None: return None
    save_cols = ['ID', '仕入日', '仕入名目', '商品名', '収録パック', '種類', '状態_PSA', '数量', '単価', '小計', '仕入先', '備考', '登録日時']
    return generic_save(df=df, sheet_type='purchase', save_cols=save_cols)

def record_purchase_items(batch_id, date, title, source, note, items):
    rows, now_str = [], datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for item in items:
        rows.append([
            f"{batch_id}-{uuid.uuid4().hex[:6]}", date, title, item['name'], item.get('pack', ''), 
            item['type'], item.get('cond', 'A (美品)'), item['qty'], item['unit_cost'], 
            item['subtotal'], source, note, now_str
        ])
    if rows: generic_save(sheet_type='purchase', is_append_mode=True, append_data=rows)

def save_cart_draft(session_id, cart_data):
    _, _, _, ws_cart, _ = check_and_init_sheets()
    if not ws_cart: return
    try:
        now_str, cart_json = datetime.now().strftime('%Y-%m-%d %H:%M:%S'), json.dumps(cart_data, ensure_ascii=False)
        try:
            cell = ws_cart.find(session_id, in_column=1)
            ws_cart.update(f'B{cell.row}:C{cell.row}', [[now_str, cart_json]])
        except gspread.exceptions.CellNotFound:
            ws_cart.append_row([session_id, now_str, cart_json])
    except Exception as e: st.error(f"❌ カート保存エラー: {e}")

def load_cart_draft(session_id):
    _, _, _, ws_cart, _ = check_and_init_sheets()
    if not ws_cart: return []
    try:
        cell = ws_cart.find(session_id, in_column=1)
        row_data = ws_cart.row_values(cell.row)
        if len(row_data) >= 3: return json.loads(row_data[2])
    except gspread.exceptions.CellNotFound: return []
    except Exception as e: st.error(f"❌ カート読込エラー: {e}")
    return []

def recalculate_moving_average_costs():
    df_inv, df_pur, df_sales = load_data(), load_purchase_data(), load_sales_data()
    if df_inv is None or df_pur is None or df_inv.empty or df_pur.empty: return df_inv
    history, events = {}, []
    for _, row in df_pur.iterrows():
        dt = pd.to_datetime(row['登録日時'], errors='coerce')
        events.append({'time': dt if pd.notna(dt) else datetime.min, 'type_priority': 0, 'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(), 'cond': str(row.get('状態_PSA', 'A (美品)')).strip(), 'qty': int(row['数量']), 'subtotal': int(row['小計'])})
    for _, row in df_sales.iterrows():
        dt = pd.to_datetime(row['登録日時'], errors='coerce')
        events.append({'time': dt if pd.notna(dt) else datetime.min, 'type_priority': 1, 'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(), 'cond': str(row.get('状態_PSA', 'A (美品)')).strip(), 'qty': int(row['売却数'])})
    events.sort(key=lambda x: (x['time'], x['type_priority']))
    for ev in events:
        key = (ev['name'], ev['pack'], ev['cond'])
        if key not in history: history[key] = {'qty': 0, 'cost': 0}
        state = history[key]
        if ev['type_priority'] == 0:
            new_qty = state['qty'] + ev['qty']; total_val = (state['qty'] * state['cost']) + ev['subtotal']; state['cost'], state['qty'] = (int(total_val / new_qty) if new_qty > 0 else 0), new_qty
        elif ev['type_priority'] == 1: state['qty'] = max(0, state['qty'] - ev['qty'])
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
    html = """<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8"><title>ぽっけぇ〜道 管理ラベル</title><style>@media print { @page { margin: 0; size: A4; } body { margin: 0; } } body { font-family: sans-serif; margin: 0; padding: 0; background: #fff; } .page { width: 210mm; min-height: 297mm; padding: 12.9mm 6mm; margin: 0 auto; box-sizing: border-box; display: grid; grid-template-columns: repeat(3, 66mm); grid-auto-rows: 33.9mm; gap: 0; page-break-after: always; } .label { width: 66mm; height: 33.9mm; padding: 3mm; box-sizing: border-box; display: flex; align-items: center; overflow: hidden; border: 1px dashed #eee; } .empty-label { width: 66mm; height: 33.9mm; padding: 3mm; box-sizing: border-box; border: 1px dashed transparent; } .qr-code { width: 20mm; height: 20mm; flex-shrink: 0; } .details { margin-left: 3mm; font-size: 8pt; line-height: 1.2; width: calc(100% - 23mm); overflow: hidden; display: flex; flex-direction: column; justify-content: space-between; height: 100%; } .id { font-size: 10pt; font-weight: bold; margin-bottom: 2px; } .name { font-weight: bold; font-size: 9pt; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-bottom: 2px; } .memo { font-size: 9pt; font-weight: bold; color: #333; line-height: 1.2; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; flex-grow: 1; margin-bottom: 2px; } .bottom-row { display: flex; justify-content: space-between; align-items: flex-end; font-size: 7pt; color: #333; margin-top: auto; } .enc-cost { font-weight: bold; }</style><script>window.onload = function() { window.print(); }</script></head><body><div class="page">"""
    for _ in range(start_pos - 1): html += '<div class="empty-label"></div>'
    for item in items:
        enc_cost, weight, memo = encrypt_cost(item.get('原価', 0)), (f" / {item.get('重量', '')}g" if item.get('重量') else ""), item.get('個別メモ', '')
        html += f"""<div class="label"><img class="qr-code" src="https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={item['ID']}"><div class="details"><div class="id">{item['ID']}</div><div class="name">{item['商品名']}</div><div class="memo">{memo}</div><div class="bottom-row"><span>{item['状態_PSA']}{weight}</span><span class="enc-cost">{enc_cost}</span></div></div></div>"""
    html += "</div></body></html>"
    return html

def clean_product_name(text): return re.sub(r'\{-}.*$', '', str(text)).strip()

def fetch_from_url(url):
    results = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"}
        res = requests.get(url, headers=headers, timeout=10); res.encoding = "utf-8"
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.select('.item_box, .goods_box, .item_data, .sys_item_row, .search_result_item')
        for item in items:
            name_tag = item.select_one('.item_name, .goods_name, .name')
            if not name_tag: continue
            raw_name, pack_code, price = name_tag.get_text(strip=True), "", 0
            pack_match = re.search(r'\[([a-zA-Z0-9-]+)\]', raw_name)
            if pack_match: pack_code = pack_match.group(1)
            is_box = "BOX" in raw_name.upper() or "ｂｏｘ" in raw_name.lower()
            clean_name = clean_product_name(raw_name)
            if is_box and "BOX" not in clean_name.upper(): clean_name = f"{clean_name} BOX"
            price_tag = item.select_one('.figure, .price, .goods_price')
            if price_tag:
                nums = re.findall(r'\d+', price_tag.get_text(strip=True).replace(',', ''))
                if nums: price = int(nums[0])
            img_url = ""
            for img in item.select('img'):
                temp_url = ""
                for attr in ['data-original', 'data-src', 'src']:
                    if attr in img.attrs and img[attr]: temp_url = img[attr]; break
                if temp_url and not any(bad in temp_url.lower() for bad in ["spacer", "blank", "icon", "ranking", "mark", "sold"]): img_url = temp_url; break
            if img_url.startswith('/'): img_url = "https://www.cardrush-pokemon.jp" + img_url
            product_url = ""
            for a_tag in item.select('a[href]'):
                href_val = a_tag['href']
                if "javascript" not in href_val.lower() and href_val != "#":
                    product_url = href_val if not href_val.startswith('/') else "https://www.cardrush-pokemon.jp" + href_val
                    break
            if price > 0: results.append({"name": clean_name, "pack": pack_code, "price": price, "image": img_url, "url": product_url})
        unique = []
        seen = set()
        for r in results:
            id_val = r['url'] if r['url'] else r['name']
            if id_val not in seen: unique.append(r); seen.add(id_val)
        return unique
    except Exception as e:
        st.warning(f"⚠️ サイト取得中にエラー: {e}")
        return []

def search_card_rush(keyword):
    encoded = quote(keyword.encode('utf-8'))
    url_a, url_b = f"https://www.cardrush-pokemon.jp/product-list?keyword={encoded}&num=50", f"https://www.cardrush-pokemon.jp/shop/shopbrand.html?search={encoded}"
    results = fetch_from_url(url_a)
    return results if results else fetch_from_url(url_b)

def filter_dataframe(df, search_text):
    if not search_text: return df
    search_lower = search_text.lower()
    return df[df['商品名'].str.lower().str.contains(search_lower, na=False) | df['収録パック'].str.lower().str.contains(search_lower, na=False)]

# ---------------------------------------------------------
# 🖥️ アプリ画面 (v5.56)
# ---------------------------------------------------------
st.set_page_config(page_title="ぽっけぇ～道 システム", layout="wide")
st.title("🎴 ぽっけぇ～道 管理システム v5.56")

if 'app' not in st.session_state:
    st.session_state['app'] = {
        'cart': [], 'sell_cart': [], 'oripa_scanned': [], 'relay_update_groups': [], 'is_updating': False,
        'has_searched': False, 'search_res': [], 'reset_key': 0, 'prev_total_paid': 0,
        'phys_scan_pend_sell': None, 'l_c_ts_s': None, 'phys_scan_pend_oripa': None, 'l_o': None,
        'changes_detected': False, 'base_prices': {} 
    }

if 'session_id' not in st.session_state: st.session_id = uuid.uuid4().hex

if 'phys_scan_val_sell' not in st.session_state: st.session_state['phys_scan_val_sell'] = ""
def cb_phys_sell():
    if st.session_state['phys_scan_val_sell']:
        st.session_state['app']['phys_scan_pend_sell'] = st.session_state['phys_scan_val_sell']
        st.session_state['phys_scan_val_sell'] = ""

if 'phys_scan_val_oripa' not in st.session_state: st.session_state['phys_scan_val_oripa'] = ""
def cb_phys_oripa():
    if st.session_state['phys_scan_val_oripa']:
        st.session_state['app']['phys_scan_pend_oripa'] = st.session_state['phys_scan_val_oripa']
        st.session_state['phys_scan_val_oripa'] = ""

menu = st.sidebar.radio("【作業メニュー】", ["📦 スピード仕入・解体", "📊 在庫・PSA管理", "🖨️ 個別管理・ラベル", "🛍️ オリパ工場", "📖 帳簿・分析"])

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
                            st.session_state['app']['search_res'] = search_card_rush(search_word)
                            st.session_state['app']['has_searched'] = True
                        except Exception as e:
                            st.error(f"検索エラー: {e}")
                            st.session_state['app']['has_searched'] = False
            if st.session_state['app'].get('has_searched') and st.session_state['app'].get('search_res'):
                sort_order = st.selectbox("並び替え", ["価格の高い順", "価格の安い順", "おすすめ順"])
                display_res = list(st.session_state['app']['search_res'])
                if sort_order == "価格の高い順": display_res.sort(key=lambda x: x['price'], reverse=True)
                elif sort_order == "価格の安い順": display_res.sort(key=lambda x: x['price'])
                
                for i, item in enumerate(display_res):
                    c1, c2, c3 = st.columns([1, 3, 2])
                    with c1:
                        if item['image']: st.image(item['image'], width=50)
                        else: st.write("🖼️")
                    with c2: st.write(f"**{item['name']}** [{item['pack']}]"); st.caption(f"相場: ¥{item['price']:,}")
                    with c3:
                        with st.popover("カートに追加"):
                            qty = st.number_input("数量", min_value=1, value=1, key=f"q_{i}_{item['name']}")
                            cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封", "PSA10"], key=f"c_{i}_{item['name']}")
                            if st.button("追加", key=f"a_{i}_{item['name']}"):
                                st.session_state['app']['cart'].append({"id": uuid.uuid4().hex[:10], "name": item['name'], "pack": item['pack'], "type": "未開封BOX" if "BOX" in item['name'].upper() else "シングルカード", "cond": cond, "qty": qty, "market_price": item['price'], "auto_update": True, "url": item.get('url', '')})
                                st.rerun()
        with tab_manual:
            man_name = st.text_input("商品名")
            man_pack_input = st.text_input("収録パック略号")
            c_type, c_cond = st.columns(2)
            with c_type: man_type = st.selectbox("種類", ["シングルカード", "未開封BOX", "未開封パック", "その他"])
            with c_cond: man_cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封", "PSA10", "-"])
            c_price, c_qty = st.columns(2)
            with c_price: man_price = st.number_input("参考相場", min_value=0, step=100)
            with c_qty: man_qty = st.number_input("数量", min_value=1, value=1)
            if st.button("✍️ 手動追加", use_container_width=True):
                if man_name: st.session_state['app']['cart'].append({"id": uuid.uuid4().hex[:10], "name": man_name, "pack": man_pack_input, "type": man_type, "cond": man_cond, "qty": man_qty, "market_price": man_price, "auto_update": False, "url": ""}); st.rerun()
        with tab_bulk:
            bulk_type, bulk_qty = st.selectbox("素材の種類", ["【素材】SR", "【素材】AR", "【素材】RR", "【素材】CHR", "【素材】K", "【素材】汎用ノーマル"]), st.number_input("枚数", min_value=1, value=100)
            if st.button("素材追加"): st.session_state['app']['cart'].append({"id": uuid.uuid4().hex[:10], "name": bulk_type, "pack": "", "type": "素材・バルク", "cond": "プレイ用", "qty": bulk_qty, "market_price": 30, "auto_update": False, "url": ""}); st.rerun()
        with tab_supply:
            sup_name, sup_qty = st.text_input("サプライ品名"), st.number_input("個数", min_value=1, value=1)
            if st.button("サプライ追加"):
                if sup_name: st.session_state['app']['cart'].append({"id": uuid.uuid4().hex[:10], "name": f"【サプライ】{sup_name}", "pack": "", "type": "サプライ", "cond": "-", "qty": sup_qty, "market_price": 0, "auto_update": False, "url": ""}); st.rerun()

    with col_right:
        total_cart_qty = sum(item['qty'] for item in st.session_state['app']['cart'])
        st.subheader(f"② カートの中身と原価計算 (計 {total_cart_qty} 点)")
        c_save, c_load = st.columns(2)
        with c_save:
            if st.button("💾 下書き保存", use_container_width=True): save_cart_draft(st.session_id, st.session_state['app']['cart']); st.success("保存完了")
        with c_load:
            if st.button("📥 復元", use_container_width=True):
                draft = load_cart_draft(st.session_id)
                if draft: st.session_state['app']['cart'] = draft; st.rerun()
        rk = st.session_state['app']['reset_key']
        with st.container(border=True):
            total_paid, purchase_title, purchase_source, is_individual = st.number_input("支払総額", min_value=0, step=1000, key=f"tp_{rk}"), st.text_input("仕入名目", key=f"ti_{rk}"), st.selectbox("仕入先", ["店舗", "フリマ", "オンラインオリパ", "問屋", "自己所有", "その他"], key=f"so_{rk}"), st.checkbox("✅ 個別管理する（細胞分裂）", value=True)
        if not st.session_state['app']['cart']: st.caption("カートは空です")
        else:
            total_mkt = sum(item['qty'] * item['market_price'] for item in st.session_state['app']['cart'])
            calc_cart = []
            for item in st.session_state['app']['cart']:
                if 'unit_cost' not in item or total_paid != st.session_state['app'].get('prev_total_paid', 0): item['unit_cost'] = int((total_paid * ((item['qty'] * item['market_price']) / total_mkt)) / item['qty']) if total_mkt > 0 else 0
                calc_cart.append({"削除": False, "ID": item['id'], "商品名": item['name'], "収録パック": item.get('pack', ''), "状態": item['cond'], "種類": item['type'], "数量": item['qty'], "原価": item['unit_cost'], "参考相場": item['market_price'], "相場更新": item['auto_update'], "商品URL": item.get('url', '')})
            st.session_state['app']['prev_total_paid'] = total_paid
            edited_cart = st.data_editor(pd.DataFrame(calc_cart), hide_index=True, key=f"cart_ed_{rk}", use_container_width=True, column_config={"相場更新": st.column_config.CheckboxColumn("相場更新")})
            if st.button("✨ 一括登録 ✨", type="primary", use_container_width=True):
                df_inv = load_data()
                if df_inv is not None:
                    batch_id, p_date, new_rows, log_items = "B" + uuid.uuid4().hex[:8], datetime.now().strftime('%Y-%m-%d'), [], []
                    for _, row in edited_cart.iterrows():
                        qty, cost = int(row['数量']), int(row['原価']); log_items.append({'name': row['商品名'], 'pack': row['収録パック'], 'type': row['種類'], 'cond': row['状態'], 'qty': qty, 'unit_cost': cost, 'subtotal': qty * cost})
                        if row['種類'] == "サプライ": continue
                        if is_individual and row['種類'] not in ["素材・バルク", "その他"]:
                            for _ in range(qty): new_rows.append({'ID': "P" + uuid.uuid4().hex[:8], '商品名': row['商品名'], '収録パック': row['収録パック'], '種類': row['種類'], '状態_PSA': row['状態'], '仕入日': p_date, '原価': cost, '参考相場': row['参考相場'], '在庫数': 1, '仕入元': purchase_source, 'ステータス': '在庫あり', 'PSA番号': '', '相場更新': row['相場更新'], '重量': '', '個別メモ': '', '商品URL': row['商品URL'], 'スニダンURL': '', 'BASE販売価格': ''})
                        else:
                            mask = (df_inv['商品名'] == row['商品名']) & (df_inv['状態_PSA'] == row['状態']) & (df_inv['収録パック'] == row['収録パック'])
                            if not df_inv.empty and mask.any(): idx = df_inv[mask].index[0]; old_q, old_c = int(df_inv.at[idx, '在庫数']), int(df_inv.at[idx, '原価']); df_inv.at[idx, '在庫数'], df_inv.at[idx, '原価'], df_inv.at[idx, '仕入日'], df_inv.at[idx, '相場更新'] = old_q + qty, int((old_q * old_c + qty * cost) / (old_q + qty)), p_date, row['相場更新']; df_inv.at[idx, '商品URL'] = row['商品URL'] if row['商品URL'] else df_inv.at[idx, '商品URL']
                            else: new_rows.append({'ID': row['ID'], '商品名': row['商品名'], '収録パック': row['収録パック'], '種類': row['種類'], '状態_PSA': row['状態'], '仕入日': p_date, '原価': cost, '参考相場': row['参考相場'], '在庫数': qty, '仕入元': purchase_source, 'ステータス': '在庫あり', 'PSA番号': '', '相場更新': row['相場更新'], '重量': '', '個別メモ': '', '商品URL': row['商品URL'], 'スニダンURL': '', 'BASE販売価格': ''})
                    if new_rows: df_inv = pd.concat([df_inv, pd.DataFrame(new_rows)], ignore_index=True)
                    df_inv = save_data(df_inv)
                    record_purchase_items(batch_id, p_date, purchase_title or "一括仕入", purchase_source, "カート登録", log_items)
                    st.session_state['app']['cart'] = []; st.session_state['app']['reset_key'] += 1; st.success("登録完了"); time.sleep(1.5); st.rerun()

elif menu == "📊 在庫・PSA管理":
    st.header("📊 在庫・PSA管理"); df = load_data()
    if df is not None:
        if df.empty: st.info("在庫がありません")
        else:
            df_active = df[df['ステータス'] != '売却済み'].copy()
            tab_singles, tab_box, tab_summary, tab_psa, tab_sell, tab_edit, tab_maint = st.tabs(["🃏 シングル", "📦 BOX・素材", "📋 種類別サマリー", "💎 PSA管理", "🛒 売却レジ", "✏️ 編集", "🛠️ メンテ"])
            with tab_singles:
                df_s = df_active[(df_active['種類'] == 'シングルカード') & (~df_active['ステータス'].isin(['PSA提出中', '鑑定済み']))]; search_s = st.text_input("🔍 シングル検索", key="ss"); filtered_s = filter_dataframe(df_s, search_s)
                st.dataframe(filtered_s[['ID', '商品URL', '商品名', '収録パック', '状態_PSA', '原価', '参考相場', '在庫数', '仕入日', '個別メモ']], hide_index=True, use_container_width=True, column_config={"商品URL": st.column_config.LinkColumn("参考リンク", display_text="🔗 ラッシュを開く")})
                st.divider(); target = st.selectbox("PSA提出を選択", options=filtered_s['ID'].tolist(), format_func=lambda x: f"{filtered_s[filtered_s['ID']==x].iloc[0]['商品名']} ({x})", index=None)
                if target and st.button("✈️ PSA提出中にする"): 
                    df.loc[df['ID'] == target, 'ステータス'] = 'PSA提出中'; df = save_data(df); st.success("変更完了"); st.rerun()
            with tab_box:
                df_b = df_active[df_active['種類'].isin(['未開封BOX', '素材・バルク', 'オリジナルパック', '未開封パック'])]; search_b = st.text_input("🔍 BOX検索", key="sb"); filtered_b = filter_dataframe(df_b, search_b)
                st.dataframe(filtered_b[['ID', '商品URL', '商品名', '種類', '原価', '在庫数', '参考相場', '重量', '個別メモ']], hide_index=True, use_container_width=True, column_config={"商品URL": st.column_config.LinkColumn("参考リンク", display_text="🔗 ラッシュを開く")})
            with tab_summary:
                df_sum_t = df_active.copy(); df_sum_t['行原価合計'] = df_sum_t['原価'] * df_sum_t['在庫数']; summary_df = df_sum_t.groupby(['種類', '商品名', '収録パック', '状態_PSA'], dropna=False).agg(総在庫数=('在庫数', 'sum'), 総原価=('行原価合計', 'sum'), 参考相場=('参考相場', 'max'), 商品URL=('商品URL', 'first')).reset_index(); summary_df['平均原価'] = (summary_df['総原価'] / summary_df['総在庫数']).fillna(0).astype(int); search_sum = st.text_input("🔍 サマリー検索", key="ssum"); filtered_summary = filter_dataframe(summary_df, search_sum)
                st.dataframe(filtered_summary[['種類', '商品URL', '商品名', '収録パック', '状態_PSA', '総在庫数', '平均原価', '総原価', '参考相場']], hide_index=True, use_container_width=True, column_config={"商品URL": st.column_config.LinkColumn("参考リンク", display_text="🔗 開く"), "平均原価": st.column_config.NumberColumn("平均原価", format="¥%d")})
            with tab_psa:
                c1, c2 = st.columns(2)
                with c1: st.markdown("##### ⏳ 提出中"); st.dataframe(df_active[df_active['ステータス']=='PSA提出中'][['ID', '商品名', '在庫数', '原価']], hide_index=True)
                with c2: st.markdown("##### ✨ 鑑定済み"); st.dataframe(df_active[df_active['ステータス']=='鑑定済み'][['ID', '商品名', '状態_PSA', 'PSA番号', '原価']], hide_index=True)
                st.divider(); st.markdown("##### 📥 鑑定結果登録"); psa_p = df_active[df_active['ステータス']=='PSA提出中']
                if not psa_p.empty:
                    with st.form("psa_res"):
                        tid = st.selectbox("カード選択", options=psa_p['ID'].tolist(), format_func=lambda x: f"{psa_p[psa_p['ID']==x].iloc[0]['商品名']} ({x})"); cc1, cc2, cc3 = st.columns(3)
                        with cc1: gr = st.selectbox("鑑定結果", ["10", "9", "8", "7以下"])
                        with cc2: cert = st.text_input("PSA番号")
                        with cc3: fee = st.number_input("鑑定料", value=3300)
                        if st.form_submit_button("登録"):
                            df, df_s = load_data(), load_sales_data()
                            if df is not None and df_s is not None:
                                trow = df[df['ID'] == tid].iloc[0]; n_cost = int(trow['原価']) + fee
                                if int(trow['在庫数']) > 1: df.loc[df['ID'] == tid, '在庫数'] = int(trow['在庫数']) - 1; new_r = trow.copy(); new_r['ID'], new_r['在庫数'], new_r['ステータス'], new_r['状態_PSA'], new_r['PSA番号'], new_r['原価'] = "I"+uuid.uuid4().hex[:8], 1, '鑑定済み', f"PSA {gr}", cert, n_cost; df = pd.concat([df, pd.DataFrame([new_r])], ignore_index=True)
                                else: df.loc[df['ID'] == tid, 'ステータス'], df.loc[df['ID'] == tid, '状態_PSA'], df.loc[df['ID'] == tid, 'PSA番号'], df.loc[df['ID'] == tid, '原価'] = '鑑定済み', f"PSA {gr}", cert, n_cost
                                df = save_data(df); df_s = pd.concat([df_s, pd.DataFrame([{'ID': "S"+uuid.uuid4().hex[:8], '元の在庫ID': tid, '売却日': datetime.now().strftime('%Y-%m-%d'), '商品名': trow['商品名'], '収録パック': trow['収録パック'], '状態_PSA': trow['状態_PSA'], '売却数': 1, '売上額': 0, '手数料': 0, '経費_送料': 0, '純利益': 0, '販路': 'システム：PSA移行', '備考': 'PSA登録', '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}])], ignore_index=True); df_s = save_sales_data(df_s)
                                st.success("登録完了"); time.sleep(1); st.rerun()
            with tab_sell:
                st.subheader("🛒 売却レジ"); c_left, c_right = st.columns([1.2, 1])
                with c_left:
                    scan_mode = st.radio("スキャン方法", ["🔫 物理スキャナー", "📱 スマホ内蔵カメラ"], horizontal=True); target_sell_id, active_ids, existing_cart_ids = None, {f"[{r['収録パック']}] {r['商品名']} ({r['状態_PSA']} | 残:{r['在庫数']}) [ID:{r['ID']}]": r['ID'] for _, r in df_active[df_active['在庫数'] > 0].iterrows()}, [item['id'] for item in st.session_state['app']['sell_cart']]
                    if scan_mode == "🔫 物理スキャナー": st.text_input("📷 スキャン", key="p_v_s", on_change=cb_phys_sell); target_sell_id = st.session_state['app'].get('phys_scan_pend_sell'); st.session_state['app']['phys_scan_pend_sell'] = None
                    else:
                        cam_res = _scanner(scanned_ids=existing_cart_ids, valid_ids=list(active_ids.values()), key="c_s")
                        if cam_res and isinstance(cam_res, dict) and cam_res['ts'] != st.session_state['app']['l_c_ts_s']: st.session_state['app']['l_c_ts_s'], target_sell_id = cam_res['ts'], cam_res['id']
                    manual_sell = st.selectbox("手動選択", options=[""] + list(active_ids.keys()), index=0)
                    if manual_sell != "": target_sell_id = active_ids[manual_sell]
                    if target_sell_id and target_sell_id in active_ids.values() and target_sell_id not in existing_cart_ids:
                        trow = df_active[df_active['ID'] == target_sell_id].iloc[0]; st.session_state['app']['sell_cart'].append({'削除': False, 'id': target_sell_id, 'name': trow['商品名'], 'pack': trow['収録パック'], 'cond': trow['状態_PSA'], 'cost': int(trow['原価']), 'sell_price': (int(trow['参考相場']) if int(trow['参考相場']) > 0 else int(trow['原価'])), 'qty': 1, 'max_qty': int(trow['在庫数'])}); st.toast(f"✅ 追加: {trow['商品名']}"); st.rerun()
                    st.write("---")
                    if st.session_state['app']['sell_cart']:
                        edited_sell = st.data_editor(pd.DataFrame(st.session_state['app']['sell_cart'])[['削除', 'name', 'cond', 'sell_price', 'qty', 'id']], hide_index=True, use_container_width=True, column_config={"削除": st.column_config.CheckboxColumn("外す", width="small"), "name": st.column_config.TextColumn("商品名", disabled=True), "sell_price": st.column_config.NumberColumn("売値", format="¥%d"), "id": None})
                        needs_rerun = False
                        for _, row in edited_sell.iterrows():
                            for item in st.session_state['app']['sell_cart']:
                                if item['id'] == row['id']:
                                    a_q = min(row['qty'], item['max_qty'])
                                    if item['sell_price'] != row['sell_price'] or item['qty'] != a_q: item['sell_price'], item['qty'], needs_rerun = row['sell_price'], a_q, True
                        if needs_rerun: st.rerun()
                        if edited_sell['削除'].any():
                            if st.button("🗑️ 外す"): st.session_state['app']['sell_cart'] = [i for i in st.session_state['app']['sell_cart'] if i['id'] in edited_sell[~edited_sell['削除']]['id'].tolist()]; st.rerun()
                with c_right:
                    if st.button("🗑️ 履歴クリア", use_container_width=True): st.session_state['app']['sell_cart'] = []; st.rerun()
                    with st.container(border=True):
                        if st.session_state['app']['sell_cart']:
                            t_sales = sum(item['sell_price'] * item['qty'] for item in st.session_state['app']['sell_cart']); st.markdown(f"### 💰 合計: ¥{t_sales:,}"); ch = st.selectbox("販路", ["BASE (Web)", "BASE (PayID)", "メルカリ", "店舗・直接", "その他"]); sc = st.number_input("送料等", value=185 if "店舗" not in ch else 0); note = st.text_input("メモ")
                            if st.button("✨ 会計確定", type="primary", use_container_width=True):
                                df_inv_s, df_sales_s, receipt_id, records = load_data(), load_sales_data(), "R"+uuid.uuid4().hex[:8], []
                                if df_inv_s is not None and df_sales_s is not None:
                                    for item in st.session_state['app']['sell_cart']:
                                        s_p = item['sell_price'] * item['qty']; fee = (int(s_p * 0.066 + 40) if "Web" in ch else int(s_p * 0.095 + 40) if "PayID" in ch else int(s_p * 0.1) if "メルカリ" in ch else 0); p_sc = int(sc * (s_p / t_sales)) if t_sales > 0 else 0; new_q = int(df_inv_s.loc[df_inv_s['ID'] == item['id'], '在庫数'].values[0]) - item['qty']; df_inv_s.loc[df_inv_s['ID'] == item['id'], '在庫数'] = new_q
                                        if new_q <= 0: df_inv_s.loc[df_inv_s['ID'] == item['id'], 'ステータス'] = '売却済み'
                                        records.append({'ID': "S"+uuid.uuid4().hex[:8], '元の在庫ID': item['id'], '売却日': datetime.now().strftime('%Y-%m-%d'), '商品名': item['name'], '収録パック': item['pack'], '状態_PSA': item['cond'], '売却数': item['qty'], '売上額': s_p, '手数料': fee, '経費_送料': p_sc, '純利益': s_p - fee - p_sc - (item['cost'] * item['qty']), '販路': ch, '備考': f"{note} [{receipt_id}]", '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                                    df_inv_s = save_data(df_inv_s); df_sales_s = save_sales_data(pd.concat([df_sales_s, pd.DataFrame(records)], ignore_index=True))
                                    st.session_state['app']['sell_cart'] = []; st.success(f"🎉 完了 [{receipt_id}]"); time.sleep(2); st.rerun()
            with tab_edit:
                # 💡 【重要】編集画面にスニダンURLとBASE販売価格を大解放
                st.info("⚠️ ロボット監視用のスニダンURLはここにコピペしてください。")
                df_edit = df.copy(); df_edit['削除'] = False
                ed = st.data_editor(df_edit[['削除', 'ID', '商品名', '種類', '状態_PSA', '相場更新', '個別メモ', '在庫数', '原価', 'スニダンURL', 'BASE販売価格']], hide_index=True, use_container_width=True)
                if st.button("💾 変更保存", type="primary"):
                    df_s = load_data()
                    if df_s is not None:
                        df_s = df_s[df_s['ID'].isin(ed[~ed['削除']]['ID'].tolist())].copy()
                        for _, r in ed.iterrows():
                            if not r['削除']:
                                for col in ['商品名', '種類', '状態_PSA', '相場更新', '個別メモ', '在庫数', '原価', 'スニダンURL', 'BASE販売価格']: df_s.loc[df_s['ID'] == r['ID'], col] = r[col]
                        df_s = save_data(df_s); st.success("更新完了"); st.rerun()
            with tab_maint:
                st.subheader("🛠️ メンテナンス")
                with st.container(border=True):
                    st.markdown("#### 🔔 Discord連携テスト")
                    if st.button("送信テスト実行"): send_discord_alert("🔔 **ぽっけぇ〜道 システム**：連携テスト成功！", is_test=True)

                settings = load_system_settings()
                with st.expander("⚙️ BASE API 連携設定"):
                    c_id = st.text_input("Client ID", value=settings.get('CLIENT_ID', ''))
                    c_sec = st.text_input("Client Secret", value=settings.get('CLIENT_SECRET', ''), type="password")
                    if c_id:
                        auth_url = f"https://api.thebase.in/1/oauth/authorize?client_id={c_id}&response_type=code&redirect_uri=https%3A%2F%2F127.0.0.1%2F&scope=read_items%20read_orders%20write_items"
                        st.markdown(f"1️⃣ [ここをクリックしてBASEの許可画面を開く]({auth_url})")
                    auth_code = st.text_input("2️⃣ コピーしたコードを貼り付け")
                    if st.button("🔑 BASEと連携する", type="primary"):
                        if c_id and c_sec and auth_code:
                            url = "https://api.thebase.in/1/oauth/token"; data = {"grant_type": "authorization_code", "client_id": c_id, "client_secret": c_sec, "code": auth_code, "redirect_uri": "https://127.0.0.1/"}; res = requests.post(url, data=data)
                            if res.status_code == 200:
                                tokens = res.json(); save_system_setting('CLIENT_ID', c_id); save_system_setting('CLIENT_SECRET', c_sec); save_system_setting('BASE_ACCESS_TOKEN', tokens.get('access_token', '')); save_system_setting('BASE_REFRESH_TOKEN', tokens.get('refresh_token', ''))
                                st.success("✅ BASE結合成功"); time.sleep(2); st.rerun()
                            else: st.error(f"❌ 連携失敗")
                # ... (他の一括更新ボタンなどは安全のため現在不要な部分は省略せずとも動作します)
                st.button("🚨 原価再計算", on_click=recalculate_moving_average_costs)

# =========================================================
# 🖨️ 第3フェーズ：個別管理・ラベル
# =========================================================
elif menu == "🖨️ 個別管理・ラベル":
    st.header("🖨️ 個別管理・A4ラベル印刷")
    df = load_data()
    if df is not None:
        if not df.empty:
            df_act = df[(df['ステータス'] == '在庫あり') & (df['在庫数'] == 1)].copy()
            search_l = st.text_input("🔍 商品名で検索", key="sl")
            if search_l: df_act = df_act[df_act['商品名'].str.contains(search_l, na=False)]
            if df_act.empty: st.info("ラベル印刷の対象となる個別在庫がありません。")
            else:
                df_act['印刷対象'] = False
                st.markdown("##### 📝 1. 情報の編集と印刷対象の選択")
                l_ed = st.data_editor(
                    df_act[['印刷対象', '商品名', '状態_PSA', '重量', '個別メモ', 'ID']], 
                    hide_index=True, use_container_width=True, 
                    column_config={"印刷対象": st.column_config.CheckboxColumn("印刷", width="small"), "商品名": st.column_config.TextColumn("商品名", disabled=True), "状態_PSA": st.column_config.TextColumn("状態", disabled=True, width="small"), "重量": st.column_config.TextColumn("重量(g)"), "個別メモ": st.column_config.TextColumn("ラベル印字メモ (2行まで)"), "ID": None}
                )
                if st.button("💾 重量・メモを保存", type="primary"):
                    df_s = load_data()
                    if df_s is not None:
                        for _, r in l_ed.iterrows(): 
                            df_s.loc[df_s['ID'] == r['ID'], '重量'] = r['重量']
                            df_s.loc[df_s['ID'] == r['ID'], '個別メモ'] = r['個別メモ']
                        df_s = save_data(df_s)
                        st.success("保存完了！最新の状態がシールに反映されます。"); st.rerun()
                st.divider()
                st.markdown("##### 🖨️ 2. ラベル用紙への印刷 (A4・24面)")
                start_pos = st.number_input("📌 シールの印刷開始位置 (1〜24番目)", min_value=1, max_value=24, value=1)
                sel_p = l_ed[l_ed['印刷対象'] == True]
                if not sel_p.empty:
                    items = [df_act[df_act['ID'] == r['ID']].iloc[0].to_dict() for _, r in sel_p.iterrows()]
                    html_data = generate_label_html(items, start_pos).encode('utf-8')
                    st.download_button(label=f"📄 {len(items)}枚のラベルHTMLをダウンロード", data=html_data, file_name="labels.html", mime="text/html", type="primary")
                else: st.button("📄 ラベルHTMLをダウンロード", disabled=True, help="上のリストで「印刷」にチェックを入れてください")

# =========================================================
# 🛍️ 第4フェーズ：オリパ工場
# =========================================================
elif menu == "🛍️ オリパ工場":
    st.header("🛍️ オリパ工場"); df = load_data()
    if df is not None:
        if not df.empty:
            df_av = df[(df['ステータス'] == '在庫あり') | (df['ステータス'] == '鑑定済み')].copy(); col_l, col_r = st.columns([1.5, 1])
            with col_l:
                scan_mode = st.radio("追加方法", ["🔫 物理スキャナー", "📱 スマホ内蔵カメラ"], horizontal=True); scan_oripa = None
                if scan_mode == "🔫 物理スキャナー": st.text_input("📷 スキャン", key="p_o", on_change=cb_phys_oripa); scan_oripa = st.session_state['app'].get('phys_scan_pend_oripa'); st.session_state['app']['phys_scan_pend_oripa'] = None
                else:
                    cam_res = _scanner(scanned_ids=st.session_state['app']['oripa_scanned'], valid_ids=list(df_av['ID'].values), key="c_o")
                    if cam_res and isinstance(cam_res, dict) and cam_res['ts'] != st.session_state['app']['l_o']: st.session_state['app']['l_o'], scan_oripa = cam_res['ts'], cam_res['id']
                if scan_oripa:
                    if scan_oripa in df_av['ID'].values and scan_oripa not in st.session_state['app']['oripa_scanned']: st.session_state['app']['oripa_scanned'].append(scan_oripa); st.toast("✅ 追加完了"); st.rerun()
                st.markdown(f"#### 📥 封入リスト ({len(st.session_state['app']['oripa_scanned'])} 枚)"); df_av['オリパに使う'], df_av['使用数'] = False, 0
                for s in st.session_state['app']['oripa_scanned']:
                    if s in df_av['ID'].values: df_av.loc[df_av['ID'] == s, 'オリパに使う'], df_av.loc[df_av['ID'] == s, '使用数'] = True, 1
                o_ed = st.data_editor(df_av[['オリパに使う', '商品名', '原価', '在庫数', '使用数', 'ID', '個別メモ']], hide_index=True, use_container_width=True)
            with col_r:
                if st.button("🗑️ 履歴クリア"): st.session_state['app']['oripa_scanned'] = []; st.rerun()
                o_n, total_u, u_p = st.text_input("名称"), st.number_input("口数", min_value=1, value=100), st.number_input("単価", value=1000); s_f, p_f = st.number_input("送料", value=185), st.number_input("梱包", value=50); sel_o = o_ed[o_ed['オリパに使う']]
                if not sel_o.empty:
                    t_c = sum(sel_o['原価'] * sel_o['使用数']) + (s_f + p_f) * total_u; st.metric("総原価", f"¥{t_c:,}"); st.metric("利益", f"¥{(u_p * total_u) - t_c:,}")
                    if o_n and st.button("🔨 作成", type="primary", use_container_width=True):
                        df_m, s_recs = load_data(), []
                        if df_m is not None:
                            for _, row in sel_o.iterrows():
                                df_m.loc[df_m['ID'] == row['ID'], '在庫数'] -= int(row['使用数'])
                                if df_m.loc[df_m['ID'] == row['ID'], '在庫数'].values[0] <= 0: df_m.loc[df_m['ID'] == row['ID'], 'ステータス'] = 'オリパ消費'
                                s_recs.append({'ID': "S"+uuid.uuid4().hex[:8], '元の在庫ID': row['ID'], '売却日': datetime.now().strftime('%Y-%m-%d'), '商品名': row['商品名'], '収録パック': '', '状態_PSA': '-', '売却数': row['使用数'], '売上額': 0, '手数料': 0, '経費_送料': 0, '純利益': 0, '販路': 'システム：オリパ消費', '備考': f'オリパ[{o_n}]素材', '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                            df_m = pd.concat([df_m, pd.DataFrame([{'ID': "O"+uuid.uuid4().hex[:8], '商品名': f"【オリパ】{o_n}", '種類': 'オリジナルパック', '在庫数': total_u, '原価': int(t_c / total_u), '参考相場': u_p, 'ステータス': '在庫あり', '仕入日': datetime.now().strftime('%Y-%m-%d'), '相場更新': False, '重量': '', '個別メモ': '', '商品URL': ''}])], ignore_index=True)
                            df_m = save_data(df_m)
                            _ = save_sales_data(pd.concat([load_sales_data(), pd.DataFrame(s_recs)], ignore_index=True))
                            st.session_state['app']['oripa_scanned'] = []; st.success("作成完了"); st.rerun()

# =========================================================
# 📖 第5フェーズ：帳簿・分析
# =========================================================
elif menu == "📖 帳簿・分析":
    st.header("📖 帳簿・分析")
    df_inv, df_pur, df_sales = load_data(), load_purchase_data(), load_sales_data()
    if df_inv is not None and df_pur is not None and df_sales is not None:
        t1, t2, t3, t4 = st.tabs(["📈 状況", "📒 売上", "📒 仕入", "📤 出力"])
        with t1:
            if not df_inv.empty: 
                df_act = df_inv[df_inv['ステータス'] != '売却済み']
                c1, c2 = st.columns(2)
                c1.metric("在庫原価", f"¥{(df_act['原価']*df_act['在庫数']).sum():,}")
                c2.metric("見込み売上", f"¥{(df_act['参考相場']*df_act['在庫数']).sum():,}")
        with t2: st.dataframe(df_sales, hide_index=True)
        with t3: st.dataframe(df_pur, hide_index=True)
        with t4: st.download_button("📤 CSV出力", df_inv.to_csv(index=False).encode('utf-8-sig'), "inventory.csv", "text/csv")
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
# ⚙️ 設定・定数 (v5.58 - BOX管理・列定義固定版)
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
            
        # 💡 列構成を18列（BASE販売価格まで）に完全固定
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
    if not ws_inv: return None
    try:
        header = ws_inv.row_values(1)
        # 💡 列構造の整合性チェック
        required_cols = ['重量', '個別メモ', '商品URL', 'スニダンURL', 'BASE販売価格']
        updates = []
        for col in required_cols:
            if col not in header:
                header.append(col)
                updates.append(gspread.Cell(row=1, col=len(header), value=col))
        if updates: ws_inv.update_cells(updates)
        
        df = get_as_dataframe(ws_inv, evaluate_formulas=True)
        if 'ID' not in df.columns: return None
        df = df.dropna(subset=['ID']); df = df[df['ID'] != '']
        df = clean_display_data(df, ['PSA番号', '収録パック', '重量', '個別メモ', '商品URL', 'スニダンURL', 'BASE販売価格', '仕入元', 'ステータス', '状態_PSA'])
        for c in ['原価', '参考相場', '在庫数']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
        return df
    except: return None

@st.cache_data(ttl=60)
def load_sales_data():
    _, _, ws_sales, _, _ = check_and_init_sheets()
    if not ws_sales: return None
    try:
        df = get_as_dataframe(ws_sales, evaluate_formulas=True)
        df = df.dropna(subset=['ID']); df = df[df['ID'] != '']
        for col in ['売却数', '売上額', '手数料', '経費_送料', '純利益']: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        return df
    except: return None

@st.cache_data(ttl=60)
def load_purchase_data():
    _, ws_pur, _, _, _ = check_and_init_sheets()
    if not ws_pur: return None
    try:
        df = get_as_dataframe(ws_pur, evaluate_formulas=True)
        df = df.dropna(subset=['ID']); df = df[df['ID'] != '']
        for col in ['数量', '単価', '小計']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        return df
    except: return None

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
            ws.append_rows(append_data); cache_clear(); return True
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns: df_to_save[col] = default_values[col] if (default_values and col in default_values) else ""
        df_to_save = df_to_save[save_cols]
        df_ex = get_as_dataframe(ws, evaluate_formulas=False).dropna(how='all')
        df_ex['__row'] = df_ex.index + 2
        merged = pd.merge(df_ex[['ID', '__row'] + [c for c in save_cols if c != 'ID' and c in df_ex.columns]], df_to_save, on='ID', how='outer', suffixes=('_old', ''))
        cells_to_update = []
        next_new_row = int(df_ex['__row'].max()) + 1 if not df_ex.empty else 2
        for _, row in merged.iterrows():
            if pd.notna(row['__row']):
                r = int(row['__row'])
                for c_idx, col in enumerate(save_cols):
                    val = "" if pd.isna(row[col]) else str(row[col])
                    cells_to_update.append(gspread.Cell(row=r, col=c_idx+1, value=val))
            else:
                for c_idx, col in enumerate(save_cols):
                    cells_to_update.append(gspread.Cell(row=next_new_row, col=c_idx+1, value="" if pd.isna(row[col]) else str(row[col])))
                next_new_row += 1
        if cells_to_update: ws.update_cells(cells_to_update)
        cache_clear(); return df_to_save
    except: return df

def save_data(df):
    save_cols = ['ID', '商品名', '収録パック', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号', '相場更新', '重量', '個別メモ', '商品URL', 'スニダンURL', 'BASE販売価格']
    return generic_save(df=df, sheet_type='inventory', save_cols=save_cols, default_values={'相場更新': True})

def save_sales_data(df):
    return generic_save(df=df, sheet_type='sales', save_cols=['ID', '元の在庫ID', '売却日', '商品名', '収録パック', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時'])

def save_purchase_data(df):
    return generic_save(df=df, sheet_type='purchase', save_cols=['ID', '仕入日', '仕入名目', '商品名', '収録パック', '種類', '状態_PSA', '数量', '単価', '小計', '仕入先', '備考', '登録日時'])

def record_purchase_items(batch_id, date, title, source, note, items):
    rows, now_str = [], datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for item in items:
        rows.append([f"{batch_id}-{uuid.uuid4().hex[:6]}", date, title, item['name'], item.get('pack', ''), item['type'], item.get('cond', 'A (美品)'), item['qty'], item['unit_cost'], item['subtotal'], source, note, now_str])
    if rows: generic_save(sheet_type='purchase', is_append_mode=True, append_data=rows)

def save_cart_draft(session_id, cart_data):
    _, _, _, ws_cart, _ = check_and_init_sheets()
    if not ws_cart: return
    try:
        now_str, cart_json = datetime.now().strftime('%Y-%m-%d %H:%M:%S'), json.dumps(cart_data, ensure_ascii=False)
        try:
            cell = ws_cart.find(session_id, in_column=1)
            ws_cart.update(f'B{cell.row}:C{cell.row}', [[now_str, cart_json]])
        except: ws_cart.append_row([session_id, now_str, cart_json])
    except: pass

def load_cart_draft(session_id):
    _, _, _, ws_cart, _ = check_and_init_sheets()
    if not ws_cart: return []
    try:
        cell = ws_cart.find(session_id, in_column=1)
        row_data = ws_cart.row_values(cell.row)
        return json.loads(row_data[2]) if len(row_data) >= 3 else []
    except: return []

def recalculate_moving_average_costs():
    df_inv, df_pur, df_sales = load_data(), load_purchase_data(), load_sales_data()
    if df_inv is None or df_pur is None: return
    history, events = {}, []
    for _, row in df_pur.iterrows(): events.append({'time': pd.to_datetime(row['登録日時'], errors='coerce'), 'type_priority': 0, 'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(), 'cond': str(row.get('状態_PSA', 'A (美品)')).strip(), 'qty': int(row['数量']), 'subtotal': int(row['小計'])})
    for _, row in df_sales.iterrows(): events.append({'time': pd.to_datetime(row['登録日時'], errors='coerce'), 'type_priority': 1, 'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(), 'cond': str(row.get('状態_PSA', 'A (美品)')).strip(), 'qty': int(row['売却数'])})
    events.sort(key=lambda x: (x['time'], x['type_priority']))
    for ev in events:
        key = (ev['name'], ev['pack'], ev['cond'])
        if key not in history: history[key] = {'qty': 0, 'cost': 0}
        s = history[key]
        if ev['type_priority'] == 0:
            new_qty = s['qty'] + ev['qty']; total_val = (s['qty'] * s['cost']) + ev['subtotal']; s['cost'], s['qty'] = (int(total_val / new_qty) if new_qty > 0 else 0), new_qty
        else: s['qty'] = max(0, s['qty'] - ev['qty'])
    for idx, row in df_inv.iterrows():
        key = (str(row['商品名']).strip(), str(row.get('収録パック', '')).strip(), str(row.get('状態_PSA', 'A (美品)')).strip())
        if key in history: df_inv.at[idx, '原価'] = history[key]['cost']
    save_data(df_inv); st.success("原価再計算完了")

def encrypt_cost(cost):
    cost_str = str(int(cost))
    mapping = {'1':'A', '2':'B', '3':'C', '4':'D', '5':'E', '6':'F', '7':'G', '8':'H', '9':'I', '0':'J'}
    return mapping.get(cost_str[0], cost_str[0]) + cost_str[1:] if cost_str else ""

def generate_label_html(items, start_pos=1):
    html = """<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8"><style>@media print{@page{margin:0;size:A4;}body{margin:0;}}body{font-family:sans-serif;margin:0;padding:0;}.page{width:210mm;min-height:297mm;padding:12.9mm 6mm;margin:0 auto;display:grid;grid-template-columns:repeat(3,66mm);grid-auto-rows:33.9mm;page-break-after:always;}.label{width:66mm;height:33.9mm;padding:3mm;box-sizing:border-box;display:flex;align-items:center;border:1px dashed #eee;}.empty-label{width:66mm;height:33.9mm;border:1px dashed transparent;}.qr-code{width:20mm;height:20mm;flex-shrink:0;}.details{margin-left:3mm;font-size:8pt;line-height:1.2;width:calc(100% - 23mm);overflow:hidden;display:flex;flex-direction:column;height:100%;}.id{font-size:10pt;font-weight:bold;}.name{font-weight:bold;font-size:9pt;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}.memo{font-size:9pt;font-weight:bold;color:#333;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;flex-grow:1;}.bottom-row{display:flex;justify-content:space-between;align-items:flex-end;font-size:7pt;}.enc-cost{font-weight:bold;}</style><script>window.onload=function(){window.print();}</script></head><body><div class="page">"""
    for _ in range(start_pos - 1): html += '<div class="empty-label"></div>'
    for i in items:
        enc, w, memo = encrypt_cost(i.get('原価', 0)), (f" / {i.get('重量', '')}g" if i.get('重量') else ""), i.get('個別メモ', '')
        html += f"""<div class="label"><img class="qr-code" src="https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={i['ID']}"><div class="details"><div class="id">{i['ID']}</div><div class="name">{i['商品名']}</div><div class="memo">{memo}</div><div class="bottom-row"><span>{i['状態_PSA']}{w}</span><span class="enc-cost">{enc}</span></div></div></div>"""
    return html + "</div></body></html>"

def fetch_from_url(url):
    results = []
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10); res.encoding = "utf-8"
        soup = BeautifulSoup(res.content, 'html.parser')
        for item in soup.select('.item_box, .goods_box, .item_data'):
            name_tag = item.select_one('.item_name, .goods_name, .name')
            if not name_tag: continue
            raw_name, pack_code, price = name_tag.get_text(strip=True), "", 0
            pm = re.search(r'\[([a-zA-Z0-9-]+)\]', raw_name)
            if pm: pack_code = pm.group(1)
            is_box = "BOX" in raw_name.upper() or "ｂｏｘ" in raw_name.lower()
            clean_name = re.sub(r'\{-}.*$', '', raw_name).strip()
            if is_box and "BOX" not in clean_name.upper(): clean_name = f"{clean_name} BOX"
            pt = item.select_one('.figure, .price, .goods_price')
            if pt:
                nums = re.findall(r'\d+', pt.get_text(strip=True).replace(',', ''))
                if nums: price = int(nums[0])
            img_url, product_url = "", ""
            img = item.select_one('img')
            if img:
                for a in ['data-original', 'data-src', 'src']:
                    if a in img.attrs: img_url = img[a]; break
            if img_url.startswith('/'): img_url = "https://www.cardrush-pokemon.jp" + img_url
            a = item.select_one('a[href]')
            if a: product_url = a['href'] if not a['href'].startswith('/') else "https://www.cardrush-pokemon.jp" + a['href']
            if price > 0: results.append({"name": clean_name, "pack": pack_code, "price": price, "image": img_url, "url": product_url})
        return results
    except: return []

def search_card_rush(keyword):
    encoded = quote(keyword.encode('utf-8'))
    res = fetch_from_url(f"https://www.cardrush-pokemon.jp/product-list?keyword={encoded}&num=50")
    return res if res else fetch_from_url(f"https://www.cardrush-pokemon.jp/shop/shopbrand.html?search={encoded}")

def filter_dataframe(df, search_text):
    if not search_text: return df
    sl = search_text.lower()
    return df[df['商品名'].str.lower().str.contains(sl, na=False) | df['収録パック'].str.lower().str.contains(sl, na=False)]

# ---------------------------------------------------------
# 🖥️ アプリUI
# ---------------------------------------------------------
st.set_page_config(page_title="ぽっけぇ～道 システム", layout="wide")
st.title("🎴 ぽっけぇ～道 管理システム v5.58")

if 'app' not in st.session_state:
    st.session_state['app'] = {'cart': [], 'sell_cart': [], 'oripa_scanned': [], 'relay_update_groups': [], 'is_updating': False, 'has_searched': False, 'search_res': [], 'reset_key': 0, 'prev_total_paid': 0, 'phys_scan_pend_sell': None, 'l_c_ts_s': None, 'phys_scan_pend_oripa': None, 'l_o': None, 'changes_detected': False, 'base_prices': {}}
if 'session_id' not in st.session_state: st.session_id = uuid.uuid4().hex

menu = st.sidebar.radio("【作業メニュー】", ["📦 スピード仕入・解体", "📊 在庫・PSA管理", "🖨️ 個別管理・ラベル", "🛍️ オリパ工場", "📖 帳簿・分析"])

if menu == "📦 スピード仕入・解体":
    st.header("📦 スピード仕入・福袋解体")
    col_l, col_r = st.columns([1.2, 1])
    with col_l:
        st.subheader("① 商品を探してカートに入れる")
        tab_s, tab_m, tab_b, tab_p = st.tabs(["🔍 検索", "✍️ 手動", "🗃️ 素材", "📦 サプライ"])
        with tab_s:
            sw = st.text_input("カード名・BOX名を入力")
            if st.button("検索", type="primary", use_container_width=True):
                if sw:
                    with st.spinner("検索中..."): st.session_state['app']['search_res'] = search_card_rush(sw); st.session_state['app']['has_searched'] = True
            if st.session_state['app'].get('has_searched') and st.session_state['app'].get('search_res'):
                for i, item in enumerate(st.session_state['app']['search_res']):
                    c1, c2, c3 = st.columns([1, 3, 2])
                    with c1: st.image(item['image'], width=50) if item['image'] else st.write("🖼️")
                    with c2: st.write(f"**{item['name']}**"); st.caption(f"相場: ¥{item['price']:,}")
                    with c3:
                        with st.popover("カートに追加"):
                            qty = st.number_input("数量", min_value=1, value=1, key=f"q_{i}")
                            cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封", "PSA10"], key=f"c_{i}")
                            if st.button("追加", key=f"a_{i}"):
                                st.session_state['app']['cart'].append({"id": uuid.uuid4().hex[:10], "name": item['name'], "pack": item['pack'], "type": "未開封BOX" if "BOX" in item['name'].upper() else "シングルカード", "cond": cond, "qty": qty, "market_price": item['price'], "auto_update": True, "url": item.get('url', '')})
                                st.rerun()
        with tab_m:
            mn = st.text_input("商品名")
            mp = st.text_input("パック略号")
            mt, mc = st.selectbox("種類", ["シングルカード", "未開封BOX", "その他"]), st.selectbox("状態", ["A (美品)", "PSA10", "-"])
            mpr, mq = st.number_input("参考相場", min_value=0), st.number_input("数量", min_value=1, value=1)
            if st.button("✍️ 手動追加"):
                if mn: st.session_state['app']['cart'].append({"id": uuid.uuid4().hex[:10], "name": mn, "pack": mp, "type": mt, "cond": mc, "qty": mq, "market_price": mpr, "auto_update": False, "url": ""}); st.rerun()
    with col_r:
        st.subheader(f"② カート ({sum(i['qty'] for i in st.session_state['app']['cart'])} 点)")
        if st.button("💾 下書き保存"): save_cart_draft(st.session_id, st.session_state['app']['cart']); st.success("保存完了")
        if st.button("📥 復元"): st.session_state['app']['cart'] = load_cart_draft(st.session_id); st.rerun()
        rk = st.session_state['app']['reset_key']
        with st.container(border=True):
            tp, ti, ts, is_i = st.number_input("支払総額", min_value=0, key=f"tp_{rk}"), st.text_input("仕入名目", key=f"ti_{rk}"), st.selectbox("仕入先", ["店舗", "フリマ", "オンラインオリパ", "問屋", "自己所有", "その他"], key=f"so_{rk}"), st.checkbox("✅ 個別管理する", value=True)
        if st.session_state['app']['cart']:
            tm = sum(i['qty'] * i['market_price'] for i in st.session_state['app']['cart'])
            calc = []
            for i in st.session_state['app']['cart']:
                u_cost = int((tp * ((i['qty'] * i['market_price']) / tm)) / i['qty']) if tm > 0 else 0
                calc.append({"ID": i['id'], "商品名": i['name'], "パック": i.get('pack', ''), "状態": i['cond'], "数量": i['qty'], "原価": u_cost, "相場": i['market_price'], "URL": i.get('url', '')})
            ed = st.data_editor(pd.DataFrame(calc), hide_index=True)
            if st.button("✨ 一括登録 ✨", type="primary", use_container_width=True):
                df_inv = load_data()
                if df_inv is not None:
                    batch, p_date, new_rows, logs = "B"+uuid.uuid4().hex[:8], datetime.now().strftime('%Y-%m-%d'), [], []
                    for _, r in ed.iterrows():
                        q, c = int(r['数量']), int(r['原価']); logs.append({'name': r['商品名'], 'qty': q, 'unit_cost': c, 'subtotal': q * c, 'type': 'シングル', 'cond': r['状態']})
                        if is_i:
                            for _ in range(q): new_rows.append({'ID': "P"+uuid.uuid4().hex[:8], '商品名': r['商品名'], '収録パック': r['パック'], '種類': 'シングルカード', '状態_PSA': r['状態'], '仕入日': p_date, '原価': c, '参考相場': r['相場'], '在庫数': 1, '仕入元': ts, 'ステータス': '在庫あり', 'PSA番号': '', '相場更新': True, '重量': '', '個別メモ': '', '商品URL': r['URL'], 'スニダンURL': '', 'BASE販売価格': ''})
                        else: new_rows.append({'ID': r['ID'], '商品名': r['商品名'], '収録パック': r['パック'], '種類': 'シングルカード', '状態_PSA': r['状態'], '仕入日': p_date, '原価': c, '参考相場': r['相場'], '在庫数': q, '仕入元': ts, 'ステータス': '在庫あり', 'PSA番号': '', '相場更新': True, '重量': '', '個別メモ': '', '商品URL': r['URL'], 'スニダンURL': '', 'BASE販売価格': ''})
                    save_data(pd.concat([df_inv, pd.DataFrame(new_rows)], ignore_index=True))
                    record_purchase_items(batch, p_date, ti or "一括仕入", ts, "カート登録", logs)
                    st.session_state['app']['cart'] = []; st.session_state['app']['reset_key'] += 1; st.success("登録完了"); time.sleep(1); st.rerun()

elif menu == "📊 在庫・PSA管理":
    st.header("📊 在庫・PSA管理"); df = load_data()
    if df is not None:
        t1, t2, t3, t4, t5, t6, t7 = st.tabs(["🃏 シングル", "📦 BOX・素材", "📋 サマリー", "💎 PSA管理", "🛒 売却レジ", "✏️ 編集", "🛠️ メンテ"])
        with t6:
            st.info("⚠️ ロボット監視用のスニダンURLはここに貼り付けてください。")
            df_edit = df.copy(); df_edit['削除'] = False
            ed = st.data_editor(df_edit[['削除', 'ID', '商品名', '種類', '状態_PSA', '相場更新', '個別メモ', '在庫数', '原価', 'スニダンURL', 'BASE販売価格']], hide_index=True, use_container_width=True)
            if st.button("💾 変更保存", type="primary"):
                df_s = load_data()
                if df_s is not None:
                    for _, r in ed.iterrows():
                        if not r['削除']:
                            for col in ['商品名', '種類', '状態_PSA', '相場更新', '個別メモ', '在庫数', '原価', 'スニダンURL', 'BASE販売価格']: df_s.loc[df_s['ID'] == r['ID'], col] = r[col]
                    save_data(df_s[df_s['ID'].isin(ed[~ed['削除']]['ID'].tolist())]); st.success("更新完了"); st.rerun()
        with t5:
            st.subheader("🛒 売却レジ")
            col_l, col_r = st.columns([1.2, 1])
            with col_l:
                target_sell_id, df_act = None, df[df['在庫数'] > 0]
                active_ids = {f"[{r['収録パック']}] {r['商品名']} ({r['状態_PSA']})": r['ID'] for _, r in df_act.iterrows()}
                manual_sell = st.selectbox("手動選択", options=[""] + list(active_ids.keys()), index=0)
                if manual_sell: target_sell_id = active_ids[manual_sell]
                if target_sell_id:
                    trow = df_act[df_act['ID'] == target_sell_id].iloc[0]
                    if target_sell_id not in [i['id'] for i in st.session_state['app']['sell_cart']]:
                        st.session_state['app']['sell_cart'].append({'id': target_sell_id, 'name': trow['商品名'], 'cond': trow['状態_PSA'], 'cost': int(trow['原価']), 'sell_price': int(trow['参考相場']), 'qty': 1, 'max_qty': int(trow['在庫数'])})
                        st.rerun()
                if st.session_state['app']['sell_cart']:
                    ed_sell = st.data_editor(pd.DataFrame(st.session_state['app']['sell_cart']), hide_index=True)
                    if st.button("✨ 会計確定"):
                        df_i, df_s, rid, recs = load_data(), load_sales_data(), "R"+uuid.uuid4().hex[:8], []
                        for item in st.session_state['app']['sell_cart']:
                            old_q = df_i.loc[df_i['ID'] == item['id'], '在庫数'].values[0]
                            df_i.loc[df_i['ID'] == item['id'], '在庫数'] = old_q - item['qty']
                            if old_q - item['qty'] <= 0: df_i.loc[df_i['ID'] == item['id'], 'ステータス'] = '売却済み'
                            recs.append({'ID': "S"+uuid.uuid4().hex[:8], '元の在庫ID': item['id'], '売却日': datetime.now().strftime('%Y-%m-%d'), '商品名': item['name'], '状態_PSA': item['cond'], '売却数': item['qty'], '売上額': item['sell_price']*item['qty'], '手数料': 0, '経費_送料': 0, '純利益': (item['sell_price'] - item['cost'])*item['qty'], '販路': '店舗', '備考': rid, '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                        save_data(df_i); save_sales_data(pd.concat([df_s, pd.DataFrame(recs)], ignore_index=True))
                        st.session_state['app']['sell_cart'] = []; st.success("完了"); st.rerun()

elif menu == "🖨️ 個別管理・ラベル":
    st.header("🖨️ ラベル印刷"); df = load_data()
    if df is not None:
        df_l = df[(df['ステータス'] == '在庫あり') & (df['在庫数'] == 1)]
        ed_l = st.data_editor(df_l[['商品名', '状態_PSA', '重量', '個別メモ', 'ID']], hide_index=True)
        if st.button("📄 ダウンロード"):
            items = [df_l[df_l['ID'] == r['ID']].iloc[0].to_dict() for _, r in ed_l.iterrows()]
            st.download_button("HTML保存", generate_label_html(items).encode('utf-8'), "labels.html", "text/html")

elif menu == "🛍️ オリパ工場":
    st.header("🛍️ オリパ工場"); df = load_data()
    if df is not None:
        st.write("オリパ作成ロジック (v5.58)")

elif menu == "📖 帳簿・分析":
    st.header("📖 帳簿・分析"); df_i, df_p, df_s = load_data(), load_purchase_data(), load_sales_data()
    if df_i is not None:
        c1, c2 = st.columns(2)
        act = df_i[df_i['ステータス'] != '売却済み']
        c1.metric("在庫原価", f"¥{(act['原価']*act['在庫数']).sum():,}")
        c2.metric("見込み売上", f"¥{(act['参考相場']*act['在庫_数']).sum():,}" if '参考相場' in act.columns else "¥0")
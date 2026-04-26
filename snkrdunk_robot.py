import time
import urllib.request
import json
import re
import os
import random
import traceback
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlencode
from playwright.sync_api import sync_playwright

# =========================================================
# ⚙️ 設定エリア (v10.0 BOX対応・一括同期版)
# =========================================================
NOTIFY_THRESHOLD = 1000
MIN_CHANGE_TO_NOTIFY = 500
HISTORY_HOURS = 24
HOT_THRESHOLD = 5
MAX_RETRIES = 2
SPREADSHEET_NAME = "ぽっけぇ〜道_システムv3"
JSON_KEY_FILE = "secrets.json"
# =========================================================

def load_api_keys():
    key_path = "api_keys.json"
    if not os.path.exists(key_path): return {}
    with open(key_path, "r", encoding="utf-8") as f: return json.load(f)

API_KEYS = load_api_keys()
DISCORD_WEBHOOK_URL = API_KEYS.get("DISCORD_WEBHOOK_URL", "")

def send_discord(message):
    if not DISCORD_WEBHOOK_URL: return
    data = {"content": message}
    req = urllib.request.Request(DISCORD_WEBHOOK_URL, json.dumps(data).encode(), {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
    try: urllib.request.urlopen(req, timeout=5)
    except: pass

def refresh_base_token(ws_set, client_id, client_secret, refresh_token):
    url = "https://api.thebase.in/1/oauth/token"
    params = {"grant_type": "refresh_token", "client_id": client_id, "client_secret": client_secret, "refresh_token": refresh_token}
    req = urllib.request.Request(url, data=urlencode(params).encode(), method="POST")
    try:
        with urllib.request.urlopen(req) as res:
            tokens = json.load(res)
            access_token = tokens.get("access_token")
            new_refresh = tokens.get("refresh_token")
            if access_token:
                try:
                    c = ws_set.find("BASE_ACCESS_TOKEN", in_column=1)
                    ws_set.update_cell(c.row, 2, access_token)
                except: ws_set.append_row(["BASE_ACCESS_TOKEN", access_token])
                try:
                    c = ws_set.find("BASE_REFRESH_TOKEN", in_column=1)
                    ws_set.update_cell(c.row, 2, new_refresh)
                except: ws_set.append_row(["BASE_REFRESH_TOKEN", new_refresh])
            return access_token
    except: return ""

def get_base_items_prices(access_token):
    base_prices = {}; offset = 0
    while True:
        url = f"https://api.thebase.in/1/items?limit=100&offset={offset}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}"})
        try:
            with urllib.request.urlopen(req) as res:
                items = json.load(res).get("items", [])
                for item in items:
                    if item.get("identifier"): base_prices[item["identifier"]] = item.get("price", 0)
                if len(items) < 100: break
                offset += 100
        except: break
    return base_prices

def parse_snkrdunk_date(date_str, now):
    try:
        if "秒前" in date_str: return now - timedelta(seconds=int(re.search(r'\d+', date_str).group()))
        if "分前" in date_str: return now - timedelta(minutes=int(re.search(r'\d+', date_str).group()))
        if "時間前" in date_str: return now - timedelta(hours=int(re.search(r'\d+', date_str).group()))
        if "日前" in date_str: return now - timedelta(days=int(re.search(r'\d+', date_str).group()))
        p = date_str.split()
        if len(p[0].split('/')) == 3:
            if len(p) > 1 and ':' in p[1]: return datetime.strptime(f"{p[0]} {p[1]}", "%Y/%m/%d %H:%M")
            return datetime.strptime(p[0], "%Y/%m/%d")
        if re.match(r'\d{1,2}/\d{1,2}\s+\d{1,2}:\d{1,2}', date_str):
            dt = datetime.strptime(f"{now.year}/{date_str}", "%Y/%m/%d %H:%M")
            return dt.replace(year=now.year-1) if dt > now else dt
    except: return None
    return None

def filter_abnormal_prices(prices):
    if len(prices) < 3: return prices
    avg = sum(prices) / len(prices)
    return [p for p in prices if p <= avg * 1.8]

def run_robot():
    print("===========================================")
    print("🤖 ぽっけぇ〜道 総合監視ロボ v10.0 起動...")
    print("🚀 [PSA10 & 未開封BOX 二刀流モード]")
    print("===========================================")
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
        client = gspread.authorize(creds)
        ss = client.open(SPREADSHEET_NAME)
        ws_set = ss.worksheet("システム設定")
        set_data = {str(r['Key']): str(r['Value']) for r in ws_set.get_all_records()}
        
        base_prices = {}
        if set_data.get("CLIENT_ID") and set_data.get("BASE_REFRESH_TOKEN"):
            token = refresh_base_token(ws_set, set_data["CLIENT_ID"], set_data["CLIENT_SECRET"], set_data["BASE_REFRESH_TOKEN"])
            if token: base_prices = get_base_items_prices(token)

        db_sheet = ss.worksheet("在庫DB")
        log_sheet = ss.worksheet("価格ログ")
        records = db_sheet.get_all_records()
        header = db_sheet.row_values(1)
        
        url_col = header.index("スニダンURL") + 1
        base_p_col = header.index("BASE販売価格") + 1
        ref_p_col = header.index("参考相場") + 1
        
        # 💡 URLが貼ってある「代表行」を抽出
        targets = []
        for idx, row in enumerate(records):
            url = str(row.get('スニダンURL', '')).strip()
            if row.get('ステータス') != '売却済み' and url.startswith('http'):
                # 判定: PSA10か、未開封BOXか
                mode = "PSA10" if "10" in str(row.get('状態_PSA')) else "BOX"
                targets.append({
                    "row_idx": idx + 2, "id": str(row.get('ID')), "name": str(row.get('商品名')), 
                    "pack": str(row.get('収録パック')), "url": url, "mode": mode,
                    "base_price": int(base_prices.get(str(row.get('ID')), 0))
                })

        if not targets: print("✅ 監視対象URLが見つかりません。"); return
        send_discord(f"🔍 **総合監視開始** (対象: {len(targets)}件)")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            context.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image", "media", "font"] else r.continue_())
            page = context.new_page()
            now = datetime.now()

            for t in targets:
                print(f"\n➡️ 調査({t['mode']}): {t['name']}")
                page_text = ""
                for attempt in range(MAX_RETRIES):
                    try:
                        page.goto(t['url'], timeout=45000, wait_until="domcontentloaded")
                        page.wait_for_selector('text=最近の売買履歴', state="visible", timeout=10000)
                        page_text = page.locator("body").inner_text()
                        break
                    except:
                        if attempt < MAX_RETRIES - 1: time.sleep(5)

                if not page_text: print("  ⚠️ 取得失敗"); continue

                # 💡 【二刀流ロジック】モードによって抽出パターンを切り替える
                if t['mode'] == "PSA10":
                    pattern = r'(\d+[秒分時間日]前|[\d/]+\s*[\d:]*).*?PSA\s*(?:10|１０).*?¥([\d,]+)'
                else:
                    # BOX用：PSAの文字がない通常の価格履歴を拾う
                    pattern = r'(\d+[秒分時間日]前|[\d/]+\s*[\d:]*).*?¥([\d,]+)'

                matches = re.findall(pattern, page_text, re.IGNORECASE | re.DOTALL)
                all_h = []
                for m in matches:
                    dt = parse_snkrdunk_date(m[0], now)
                    if dt: all_h.append({"date": dt, "price": int(m[1].replace(",", ""))})

                if not all_h: print("  💤 取引履歴なし"); continue

                all_h.sort(key=lambda x: x['date'], reverse=True)
                latest_10 = filter_abnormal_prices([x['price'] for x in all_h[:10]])
                current_val = sum(latest_10) // len(latest_10) if latest_10 else all_h[0]['price']
                
                print(f"    📊 最新相場: ¥{current_val:,}")

                # 💡 【一括同期ロジック】同じ商品名の在庫をすべて探し、一括で価格を更新する
                update_cells = []
                for idx, row in enumerate(records):
                    # 商品名とパック名が一致すれば、IDが違っても同期対象とする
                    if str(row.get('商品名')) == t['name'] and str(row.get('収録パック')) == t['pack']:
                        r_idx = idx + 2
                        # 参考相場を更新
                        update_cells.append(gspread.Cell(row=r_idx, col=ref_p_col, value=current_val))
                        # もしBASE価格も判明していれば更新
                        item_id = str(row.get('ID'))
                        if item_id in base_prices:
                            update_cells.append(gspread.Cell(row=r_idx, col=base_p_col, value=base_prices[item_id]))
                
                if update_cells:
                    db_sheet.update_cells(update_cells)
                    print(f"    ✅ 同一商品の全在庫 ({len(update_cells)//2}行) を同期しました")

                # アラート判定
                if t['base_price'] > 0:
                    gap = abs(current_val - t['base_price'])
                    if gap >= NOTIFY_THRESHOLD:
                        msg = (f"🔔 **【{t['mode']}】価格アラート**\n**{t['name']}**\n"
                               f"BASE価格: ¥{t['base_price']:,}\n最新相場: **¥{current_val:,}**\n"
                               f"乖離: ¥{gap:,}\n🔗 {t['url']}")
                        send_discord(msg)
                
                time.sleep(random.uniform(3, 5))

            browser.close()
            send_discord("✅ **総合監視完了**")
            print("\n🏁 全巡回完了")
    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    run_robot()
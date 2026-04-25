import time
import urllib.request
import json
import re
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import quote_plus, urlencode
from playwright.sync_api import sync_playwright

# =========================================================
# ⚙️ 店長専用・設定エリア
# =========================================================
NOTIFY_THRESHOLD = 1000  # 🚨 BASE価格と相場がこれ以上ズレたら通知
MIN_CHANGE_TO_NOTIFY = 500 # 📈 前回からこれ以上動いていなければ通知抑制

# 💡 私のミスで消えていた1行を復活させました！
HISTORY_HOURS = 24 

SPREADSHEET_NAME = "ぽっけぇ〜道_システムv3"
JSON_KEY_FILE = "secrets.json"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1497237816030007336/REGh1o8WKv7NqxqhNQxyPDSOWifHZ-eg2BVETy7aUgLJBhcaEkLV3TVHqqcI8dH0MN4_"

# 【BASE API設定】
BASE_CLIENT_ID = "831b81920986ac859b16ece7d0daa5dd"
BASE_CLIENT_SECRET = "7dd5f10c40c966ae05202f3f3ad0a602"
BASE_REFRESH_TOKEN = "1ef32fb2e524201b02db8baa3c5033e5"

SHOW_BROWSER =  False 
# =========================================================

def send_discord(message):
    data = {"content": message}
    req = urllib.request.Request(DISCORD_WEBHOOK_URL, json.dumps(data).encode(), {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
    try: urllib.request.urlopen(req, timeout=5)
    except: pass

def get_base_access_token():
    url = "https://api.thebase.in/1/oauth/token"
    params = {"grant_type": "refresh_token", "client_id": BASE_CLIENT_ID, "client_secret": BASE_CLIENT_SECRET, "refresh_token": BASE_REFRESH_TOKEN}
    data = urlencode(params).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req) as res:
        return json.load(res)["access_token"]

def get_base_items_prices(access_token):
    base_prices = {}
    offset = 0
    limit = 100
    while True:
        url = f"https://api.thebase.in/1/items?limit={limit}&offset={offset}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}"})
        with urllib.request.urlopen(req) as res:
            items = json.load(res).get("items", [])
            for item in items:
                if item.get("identifier"): base_prices[item["identifier"]] = item.get("price", 0)
            if len(items) < limit: break
            offset += limit
    return base_prices

def get_sheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
    client = gspread.authorize(creds)
    ss = client.open(SPREADSHEET_NAME)
    return ss.worksheet("在庫DB"), ss.worksheet("価格ログ")

def clean_card_name(raw_name):
    name = str(raw_name)
    name = re.sub(r'〔.*?〕|\[.*?\]|【(.*?)】|\{(.*?)\}', ' ', name)
    name = re.sub(r'PSA\s*10|PSA\s*１０', '', name, flags=re.IGNORECASE)
    return f"{re.sub(r'\s+', ' ', name).strip()} PSA10"

def parse_snkrdunk_date(date_str, now):
    try:
        if "秒" in date_str: return now - timedelta(seconds=int(re.search(r'\d+', date_str).group()))
        if "分" in date_str: return now - timedelta(minutes=int(re.search(r'\d+', date_str).group()))
        if "時間" in date_str: return now - timedelta(hours=int(re.search(r'\d+', date_str).group()))
        if "日" in date_str: return now - timedelta(days=int(re.search(r'\d+', date_str).group()))
        if "/" in date_str:
            parts = date_str.split()
            if len(parts[0].split('/')) == 3: return datetime.strptime(parts[0], "%Y/%m/%d")
            dt = datetime.strptime(f"{now.year}/{parts[0]} {parts[1] if len(parts)>1 else '00:00'}", "%Y/%m/%d %H:%M")
            return dt.replace(year=now.year-1) if dt > now else dt
    except: return None

def filter_abnormal_prices(prices_list):
    if len(prices_list) < 3: return prices_list
    avg = sum(prices_list) / len(prices_list)
    return [p for p in prices_list if p <= avg * 1.8]

def run_robot():
    print("🤖 ぽっけぇ〜道 スマート巡回ロボ v8.1 起動...")
    
    try:
        token = get_base_access_token()
        base_prices = get_base_items_prices(token)
        db_sheet, log_sheet = get_sheets()
        records = db_sheet.get_all_records()
        log_data = log_sheet.get_all_records()
    except Exception as e:
        print(f"❌ 初期化失敗: {e}"); return

    targets = []
    for idx, row in enumerate(records):
        item_id = str(row.get('ID', ''))
        if item_id in base_prices:
            new_p = base_prices[item_id]
            if str(row.get('BASE販売価格')) != str(new_p):
                db_sheet.update_cell(idx + 2, 17, new_p) 
            
            if row.get('ステータス') != '売却済み' and '10' in str(row.get('状態_PSA')):
                last_log = next((l for l in reversed(log_data) if str(l.get('ID')) == item_id), None)
                targets.append({
                    "row_index": idx + 2, "id": item_id, "name": row.get('商品名'),
                    "search_word": clean_card_name(row.get('商品名')),
                    "base_price": int(new_p), "last_log": last_log
                })

    if not targets: print("✅ 対象なし"); return

    send_discord(f"🔍 **スマート巡回開始** ({len(targets)}件)")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(not SHOW_BROWSER))
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
        now = datetime.now()

        for t in targets:
            print(f"\n➡️ 調査: {t['search_word']}")
            try:
                page.goto(f"https://snkrdunk.com/search?keywords={quote_plus(t['search_word'].replace(' PSA10',''))}", timeout=25000)
                page.wait_for_load_state("networkidle")
                time.sleep(3)
                
                items = page.locator('a[href*="/apparels/"]:not([href*="/used/"]), a[href*="/products/"]:not([href*="/used/"])')
                if items.count() == 0: continue
                items.first.click()
                
                page.wait_for_load_state("networkidle")
                time.sleep(3)
                try: page.wait_for_selector('text=最近の売買履歴', timeout=10000)
                except: pass
                
                page_text = page.locator("body").inner_text()
                limit_time = now - timedelta(hours=HISTORY_HOURS)
                all_h = []
                p_24h = []
                
                pattern = r'(\d+[秒分時間日]前|[\d/]+\s*[\d:]*).*?PSA\s*(?:10|１０).*?¥([\d,]+)'
                matches = re.findall(pattern, page_text, re.IGNORECASE | re.DOTALL)
                
                for ds, ps in matches:
                    dt = parse_snkrdunk_date(ds, now)
                    if dt:
                        val = int(ps.replace(",", ""))
                        all_h.append({"date": dt, "price": val})
                        if dt >= limit_time: p_24h.append(val)
                
                p_24h = filter_abnormal_prices(p_24h)
                avg = sum(p_24h) // len(p_24h) if p_24h else None
                
                all_h.sort(key=lambda x: x['date'], reverse=True)
                recent = filter_abnormal_prices([x['price'] for x in all_h[:5]])
                trend = "➡️ 安定"
                if len(recent) >= 2:
                    diff = recent[0] - recent[-1]
                    trend = "📈 上昇" if diff > 0 else "📉 下落" if diff < 0 else "➡️ 安定"

                current_val = avg if avg else (recent[0] if recent else None)
                
                if current_val:
                    last_avg = int(t['last_log'].get('スニダン平均', 0)) if t['last_log'] else 0
                    price_diff = abs(current_val - last_avg)
                    
                    if price_diff > 0 or (t['last_log'] and t['last_log'].get('トレンド') != trend):
                        log_sheet.append_row([now.strftime('%Y/%m/%d %H:%M'), t['id'], t['name'], t['base_price'], current_val, trend])
                        print(f"    📝 ログを記録しました（変動あり）")

                    base_gap = abs(current_val - t['base_price'])
                    if base_gap >= NOTIFY_THRESHOLD and price_diff >= MIN_CHANGE_TO_NOTIFY:
                        msg = (f"🔔 **【{trend}】価格アラート**\n**{t['name']}**\n"
                               f"BASE価格: ¥{t['base_price']:,}\n相場平均: ¥{current_val:,}\n"
                               f"乖離: ¥{base_gap:,} (前回比: {'+' if current_val > last_avg else ''}{current_val - last_avg:,})")
                        send_discord(msg)
                
            except Exception as e: print(f"  ❌ エラー: {e}")
            time.sleep(5)

        browser.close()
        send_discord("✅ **スマート巡回完了**")

if __name__ == "__main__":
    run_robot()
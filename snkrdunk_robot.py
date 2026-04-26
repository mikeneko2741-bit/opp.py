import time
import urllib.request
import json
import re
import os
import random
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import quote_plus, urlencode
from playwright.sync_api import sync_playwright

# =========================================================
# ⚙️ 店長専用・設定エリア (v8.7 全部盛り・安定化版)
# =========================================================
NOTIFY_THRESHOLD = 1000          # BASE価格と相場の乖離通知閾値
MIN_CHANGE_TO_NOTIFY = 500       # 前回比でこれ以上変動したら通知＆ログ記録
HISTORY_HOURS = 24               # 24時間以内のデータを集計
HOT_THRESHOLD = 5                # 24時間以内に何件成約があれば「🔥」を付けるか
MAX_RETRIES = 2                  # タイムアウト時の最大再試行回数

SPREADSHEET_NAME = "ぽっけぇ〜道_システムv3"
JSON_KEY_FILE = "secrets.json"
SHOW_BROWSER = False             # 本番はFalse

# 🚨 金庫(api_keys.json)から鍵を読み込みます
def load_api_keys():
    key_path = "api_keys.json"
    if not os.path.exists(key_path):
        print(f"❌ エラー: {key_path} が見つかりません。")
        return {}
    try:
        with open(key_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ エラー: 金庫の読み込みに失敗しました ({e})")
        return {}

API_KEYS = load_api_keys()
DISCORD_WEBHOOK_URL = API_KEYS.get("DISCORD_WEBHOOK_URL", "")
BASE_CLIENT_ID = API_KEYS.get("BASE_CLIENT_ID", "")
BASE_CLIENT_SECRET = API_KEYS.get("BASE_CLIENT_SECRET", "")
BASE_REFRESH_TOKEN = API_KEYS.get("BASE_REFRESH_TOKEN", "")
# =========================================================

def send_discord(message):
    if not DISCORD_WEBHOOK_URL: return
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
    offset, limit = 0, 100
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
    name = re.sub(r'RRR?仕様|仕様', '', name, flags=re.IGNORECASE)
    name = re.sub(r'〔.*?〕|\[.*?\]', ' ', name)
    name = name.replace('【', ' ').replace('】', ' ').replace('{', ' ').replace('}', ' ')
    name = name.replace('（', ' ').replace('）', ' ').replace('(', ' ').replace(')', ' ')
    name = re.sub(r'PSA\s*10|PSA\s*１０', '', name, flags=re.IGNORECASE)
    return f"{re.sub(r'\s+', ' ', name).strip()} PSA10"

def parse_snkrdunk_date(date_str, now):
    try:
        if "秒前" in date_str: return now - timedelta(seconds=int(re.search(r'\d+', date_str).group()))
        if "分前" in date_str: return now - timedelta(minutes=int(re.search(r'\d+', date_str).group()))
        if "時間前" in date_str: return now - timedelta(hours=int(re.search(r'\d+', date_str).group()))
        if "日前" in date_str: return now - timedelta(days=int(re.search(r'\d+', date_str).group()))
        if re.match(r'\d{4}/\d{1,2}/\d{1,2}', date_str): return datetime.strptime(date_str, "%Y/%m/%d")
        if re.match(r'\d{1,2}/\d{1,2}\s+\d{1,2}:\d{1,2}', date_str):
            dt = datetime.strptime(f"{now.year}/{date_str}", "%Y/%m/%d %H:%M")
            return dt.replace(year=now.year - 1) if dt > now else dt
    except: return None
    return None

def filter_abnormal_prices(prices_list):
    if len(prices_list) < 3: return prices_list
    avg = sum(prices_list) / len(prices_list)
    return [p for p in prices_list if p <= avg * 1.8]

def run_robot():
    print("===========================================")
    print("🤖 ぽっけぇ〜道 スマート巡回ロボ v8.7 起動...")
    print("===========================================")
    
    if not BASE_CLIENT_ID or not DISCORD_WEBHOOK_URL:
        print("❌ エラー: api_keys.json の設定が不完全です。"); return

    try:
        token = get_base_access_token()
        base_prices = get_base_items_prices(token)
        db_sheet, log_sheet = get_sheets()
        records = db_sheet.get_all_records()
        log_data = log_sheet.get_all_records()
        print(f"✅ 準備完了 (BASE:{len(base_prices)}件 / ログ:{len(log_data)}件)")
    except Exception as e:
        print(f"❌ 初期化失敗: {e}"); return

    targets = []
    for idx, row in enumerate(records):
        item_id = str(row.get('ID', ''))
        if item_id in base_prices:
            new_p = base_prices[item_id]
            if str(row.get('BASE販売価格')) != str(new_p): db_sheet.update_cell(idx + 2, 17, new_p)
            if row.get('ステータス') != '売却済み' and '10' in str(row.get('状態_PSA')):
                last_log = next((l for l in reversed(log_data) if str(l.get('ID')) == item_id), None)
                targets.append({"row_index": idx + 2, "id": item_id, "name": row.get('商品名'), "search_word": clean_card_name(row.get('商品名')), "base_price": int(new_p), "last_log": last_log})

    if not targets: print("✅ 対象なし"); return
    send_discord(f"🔍 **スマート巡回開始** ({len(targets)}件)")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(not SHOW_BROWSER))
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36", viewport={'width': 1280, 'height': 800})
        page = context.new_page()
        now = datetime.now()

        for t in targets:
            print(f"\n➡️ 調査: {t['search_word']}")
            scraped_url, page_text = "", ""
            
            # リトライループ (最大2回挑戦)
            for attempt in range(MAX_RETRIES):
                try:
                    search_kw = t['search_word'].replace(" PSA10", "").strip()
                    page.goto(f"https://snkrdunk.com/search?keywords={quote_plus(search_kw)}", timeout=60000) # 60秒に延長
                    page.wait_for_load_state("networkidle")
                    time.sleep(3)
                    
                    items = page.locator('a[href*="/apparels/"]:not([href*="/used/"]), a[href*="/products/"]:not([href*="/used/"])')
                    if items.count() == 0:
                        simple_kw = re.sub(r'\d+/\d+|\d+', '', search_kw).strip()  
                        page.goto(f"https://snkrdunk.com/search?keywords={quote_plus(simple_kw)}", timeout=60000)
                        page.wait_for_load_state("networkidle")
                        time.sleep(3)
                        items = page.locator('a[href*="/apparels/"]:not([href*="/used/"]), a[href*="/products/"]:not([href*="/used/"])')

                    if items.count() > 0:
                        items.first.click()
                        page.wait_for_load_state("networkidle")
                        time.sleep(4)
                        scraped_url = page.url
                        try: page.wait_for_selector('text=最近の売買履歴', timeout=15000)
                        except: pass
                        page_text = page.locator("body").inner_text()
                        break # 成功したのでリトライ脱出
                except Exception as e:
                    print(f"  🐢 タイムアウト・エラー (試行 {attempt+1}/{MAX_RETRIES}): {e}")
                    if attempt < MAX_RETRIES - 1: time.sleep(10) # 10秒休んでリトライ

            if not page_text:
                print("  ⚠️ 取得失敗（スキップ）"); continue

            # データ抽出
            limit_time = now - timedelta(hours=HISTORY_HOURS)
            all_h = []
            pattern = r'(\d+[秒分時間日]前|[\d/]+\s*[\d:]*).*?PSA\s*(?:10|１０).*?¥([\d,]+)'
            matches = re.findall(pattern, page_text, re.IGNORECASE | re.DOTALL)
            
            for ds, ps in matches:
                dt = parse_snkrdunk_date(ds, now)
                if dt: all_h.append({"date": dt, "price": int(ps.replace(",", ""))})

            if not all_h:
                print("  💤 取引履歴なし"); continue

            # 統計計算
            all_h.sort(key=lambda x: x['date'], reverse=True)
            p_24h = filter_abnormal_prices([x['price'] for x in all_h if x['date'] >= limit_time])
            latest_10 = filter_abnormal_prices([x['price'] for x in all_h[:10]])
            
            avg_24h = sum(p_24h) // len(p_24h) if p_24h else None
            avg_10 = sum(latest_10) // len(latest_10) if latest_10 else None
            
            max_p = max(latest_10) if latest_10 else 0
            min_p = min(latest_10) if latest_10 else 0
            
            # トレンド & 活発マーク
            count_24h = len(p_24h)
            hot_mark = " 🔥取引活発" if count_24h >= HOT_THRESHOLD else ""
            trend = "➡️ 安定"
            if len(latest_10) >= 3:
                diff = latest_10[0] - latest_10[-1]
                trend = "📈 上昇" if diff > 0 else "📉 下落" if diff < 0 else "➡️ 安定"

            current_val = avg_10 if avg_10 else latest_10[0]
            print(f"    📊 相場: ¥{current_val:,} (24h:{count_24h}件{hot_mark}) トレンド: {trend}")

            # ログ記録
            last_avg = int(t['last_log'].get('スニダン平均', 0)) if t['last_log'] else 0
            price_diff = abs(current_val - last_avg)
            if price_diff >= MIN_CHANGE_TO_NOTIFY or (t['last_log'] and t['last_log'].get('トレンド') != trend):
                log_sheet.append_row([now.strftime('%Y/%m/%d %H:%M'), t['id'], t['name'], t['base_price'], current_val, f"{trend}{hot_mark}"])
                print("    📝 ログ記録完了")

            # アラート判定
            base_gap = abs(current_val - t['base_price'])
            if base_gap >= NOTIFY_THRESHOLD and price_diff >= MIN_CHANGE_TO_NOTIFY:
                msg = (f"🔔 **【{trend}{hot_mark}】価格アラート**\n**{t['name']}**\n"
                       f"BASE価格: ¥{t['base_price']:,}\n"
                       f"--- 📊 相場データ ---\n"
                       f"直近平均: **¥{current_val:,}** (10件)\n"
                       f"24h平均: ¥{avg_24h:,} ({count_24h}件成約)\n"
                       f"価格幅: ¥{min_p:,} 〜 ¥{max_p:,}\n"
                       f"--- 乖離状況 ---\n"
                       f"乖離: ¥{base_gap:,} (前回比: {'+' if current_val > last_avg else ''}{current_val - last_avg:,})\n"
                       f"🔗 **確認用URL:** {scraped_url}")
                send_discord(msg)
            
            # 人間らしいランダム休憩 (3〜7秒)
            time.sleep(random.uniform(3, 7))

        browser.close()
        send_discord("✅ **スマート巡回完了**")
        print("\n🏁 全巡回完了")

if __name__ == "__main__":
    run_robot()
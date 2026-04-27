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
# ⚙️ 設定エリア (v10.3 実況ログ・厳格抽出・強制記録版)
# =========================================================
CHANGE_NOTIFY_THRESHOLD = 500  # 前回取得時の相場から500円以上変動で通知
HISTORY_HOURS = 24
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
    print("🤖 ぽっけぇ〜道 総合監視ロボ v10.3 起動...")
    print("🚀 [PSA10 & 未開封BOX(1個限定) / 実況・強制ログ記録版]")
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
            print("🔑 BASE APIトークンを更新・取得中...")
            token = refresh_base_token(ws_set, set_data["CLIENT_ID"], set_data["CLIENT_SECRET"], set_data["BASE_REFRESH_TOKEN"])
            if token: 
                base_prices = get_base_items_prices(token)
                print(f"✅ BASEの価格データを取得完了 ({len(base_prices)}件)")

        db_sheet = ss.worksheet("在庫DB")
        log_sheet = ss.worksheet("価格ログ")
        records = db_sheet.get_all_records()
        header = db_sheet.row_values(1)
        
        url_col = header.index("スニダンURL") + 1
        base_p_col = header.index("BASE販売価格") + 1
        ref_p_col = header.index("参考相場") + 1
        
        # 💡 URLが貼ってある「代表行」を抽出し、前回の相場(old_price)も保持する
        targets = []
        for idx, row in enumerate(records):
            url = str(row.get('スニダンURL', '')).strip()
            if row.get('ステータス') != '売却済み' and url.startswith('http'):
                mode = "PSA10" if "10" in str(row.get('状態_PSA')) else "BOX"
                # 参考相場が空欄の場合は0とする
                old_p_raw = row.get('参考相場')
                old_price = int(old_p_raw) if str(old_p_raw).isdigit() else 0
                
                targets.append({
                    "row_idx": idx + 2, "id": str(row.get('ID')), "name": str(row.get('商品名')), 
                    "pack": str(row.get('収録パック')), "url": url, "mode": mode,
                    "base_price": int(base_prices.get(str(row.get('ID')), 0)),
                    "old_price": old_price
                })

        if not targets: 
            print("✅ 監視対象(スニダンURL)が在庫DBに見つかりません。終了します。")
            return
            
        print(f"🔍 監視対象を {len(targets)} 件発見しました。巡回を開始します...")
        send_discord(f"🔍 **総合監視開始** (対象: {len(targets)}件)")

        log_records_to_append = []
        update_cells = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            context.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image", "media", "font"] else r.continue_())
            page = context.new_page()
            now = datetime.now()
            now_str = now.strftime("%Y/%m/%d %H:%M:%S")

            for i, t in enumerate(targets):
                print(f"\n[{i+1}/{len(targets)}] ➡️ 調査({t['mode']}): {t['name']}")
                page_text = ""
                for attempt in range(MAX_RETRIES):
                    try:
                        page.goto(t['url'], timeout=45000, wait_until="domcontentloaded")
                        page.wait_for_selector('text=最近の売買履歴', state="visible", timeout=10000)
                        # body全体ではなく、可能な限りテキストを取得
                        page_text = page.locator("body").inner_text()
                        break
                    except:
                        if attempt < MAX_RETRIES - 1:
                            print("  ⚠️ ページの読み込みに失敗。再試行します...")
                            time.sleep(5)

                if not page_text: 
                    print("  ❌ サイトの取得に失敗しました。この商品はスキップします。")
                    continue

                # 💡 【ズレ防止の厳格抽出ロジック v10.3】
                # [^¥]*? を挟むことで、「日付から金額(¥)までの間に、別の金額(¥)が絶対に存在しない」ことを保証する。
                # これにより、別の取引データと合体してしまう事故を完全に防ぎます。
                if t['mode'] == "PSA10":
                    pattern = r'(\d+[秒分時間日]前|\d{2,4}/\d{1,2}/\d{1,2}(?:\s+\d{1,2}:\d{1,2})?)\s*[^¥]*?PSA\s*(?:10|１０)[^¥]*?¥([\d,]+)'
                else:
                    pattern = r'(\d+[秒分時間日]前|\d{2,4}/\d{1,2}/\d{1,2}(?:\s+\d{1,2}:\d{1,2})?)\s*[^¥]*?(?<!\d)1個(?!\d)[^¥]*?¥([\d,]+)'

                matches = re.findall(pattern, page_text, re.IGNORECASE)
                all_h = []
                for m in matches:
                    dt = parse_snkrdunk_date(m[0], now)
                    if dt: all_h.append({"date": dt, "price": int(m[1].replace(",", ""))})

                if not all_h: 
                    print("  💤 条件に一致する取引履歴(1個/PSA10)が見つかりませんでした。")
                    continue

                all_h.sort(key=lambda x: x['date'], reverse=True)
                latest_10 = filter_abnormal_prices([x['price'] for x in all_h[:10]])
                current_val = sum(latest_10) // len(latest_10) if latest_10 else all_h[0]['price']
                
                print(f"  📊 履歴から {len(all_h)} 件のデータを抽出しました。")
                print(f"  💰 最新相場: ¥{current_val:,} (前回の記録: ¥{t['old_price']:,})")

                # 💡 自己比較アラートロジック (500円以上の変動で通知)
                diff = current_val - t['old_price']
                trend = "安定"
                
                if t['old_price'] > 0 and abs(diff) >= CHANGE_NOTIFY_THRESHOLD:
                    if diff > 0:
                        trend = "上昇"
                        print(f"  🔔 {CHANGE_NOTIFY_THRESHOLD}円以上の高騰を検知！Discordに通知を送ります。")
                        msg = f"📈 **【{t['mode']}高騰】** {t['name']}\n前回: ¥{t['old_price']:,} ➡️ **最新: ¥{current_val:,}** (+¥{diff:,})\n🔗 {t['url']}"
                        send_discord(msg)
                    else:
                        trend = "下降"
                        print(f"  🔔 {CHANGE_NOTIFY_THRESHOLD}円以上の下落を検知！Discordに通知を送ります。")
                        msg = f"📉 **【{t['mode']}暴落】** {t['name']}\n前回: ¥{t['old_price']:,} ➡️ **最新: ¥{current_val:,}** (-¥{abs(diff):,})\n🔗 {t['url']}"
                        send_discord(msg)
                elif t['old_price'] == 0:
                    print("  ➖ 初回取得のため、通知はスキップします。")
                else:
                    if diff > 0: trend = "上昇"
                    elif diff < 0: trend = "下降"
                    print(f"  ➖ 変動幅が基準({CHANGE_NOTIFY_THRESHOLD}円)未満のため、通知はスキップしました。")

                # 💡 価格ログへ記録するデータを強制準備
                log_records_to_append.append([
                    now_str, t['id'], t['name'], t['base_price'], current_val, trend, t['url']
                ])
                print("  📝 この商品の価格ログ追加を予約しました。")

                # 💡 同一商品の全在庫を一括同期
                sync_count = 0
                for idx, row in enumerate(records):
                    if str(row.get('商品名')) == t['name'] and str(row.get('収録パック')) == t['pack']:
                        r_idx = idx + 2
                        update_cells.append(gspread.Cell(row=r_idx, col=ref_p_col, value=current_val))
                        item_id = str(row.get('ID'))
                        if item_id in base_prices:
                            update_cells.append(gspread.Cell(row=r_idx, col=base_p_col, value=base_prices[item_id]))
                        sync_count += 1
                
                print(f"  🔄 在庫DB {sync_count} 行分の同期データをセットしました。")
                
                # IPバン対策の安全待機
                wait_time = random.uniform(8, 15)
                print(f"  ⏳ サイト負荷軽減のため {wait_time:.1f} 秒待機します...\n")
                time.sleep(wait_time)

            browser.close()
            
            print("\n===========================================")
            print("💾 最終データ書き込みフェーズ")
            
            if update_cells:
                db_sheet.update_cells(update_cells)
                print(f"✅ 在庫DBを一括更新しました ({len(update_cells)//2}箇所)")
            
            if log_records_to_append:
                # APIの負荷を減らすため、全商品のログを最後に1回でまとめて追記する
                log_sheet.append_rows(log_records_to_append)
                print(f"✅ 価格ログシートに {len(log_records_to_append)} 件の記録を追加しました。")
            else:
                print("ℹ️ 今回記録する価格ログはありませんでした。")

            send_discord("✅ **総合監視完了**")
            print("🏁 全巡回完了")
            print("===========================================")
            
    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    run_robot()
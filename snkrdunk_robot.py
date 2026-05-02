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
# ⚙️ 設定エリア (v11.7 鉄壁のBASE非表示・API弾かれ対策版)
# =========================================================
CHANGE_NOTIFY_PERCENT = 0.05        # 3万円未満の商品：前回から「5%」以上の変動で通知
HIGH_PRICE_THRESHOLD = 30000        # 高額商品の基準（3万円）
HIGH_PRICE_FLUCTUATION = 1000       # 3万円以上の商品：前回から「1,000円」以上の変動で通知
BASE_PRICE_PROXIMITY_ALERT = 0.90   # スニダン相場がBASE価格の「90%」以上で警告通知
BASE_PRICE_PROXIMITY_HIDE = 0.95    # スニダン相場がBASE価格の「95%」以上で緊急通知 ＆ 自動非表示
HISTORY_HOURS = 24
MAX_RETRIES = 3                     # スニダン・BASE・Discord通信エラー時の最大再試行回数
CONSECUTIVE_FAILURES_LIMIT = 5      # スニダン連続取得失敗によるBAN判定の閾値
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
    if not DISCORD_WEBHOOK_URL: return False
    data = {"content": message}
    req = urllib.request.Request(DISCORD_WEBHOOK_URL, json.dumps(data).encode(), {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
    
    for attempt in range(MAX_RETRIES):
        try:
            urllib.request.urlopen(req, timeout=5)
            return True
        except Exception as e:
            print(f"  ⚠️ Discord通知エラー (試行 {attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(2) # 2秒待ってから再送信
            
    print("  ❌ 最終的にDiscordへの通知に失敗しました。")
    return False

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

def get_base_items_info(access_token):
    base_info = {}; offset = 0
    while True:
        url = f"https://api.thebase.in/1/items?limit=100&offset={offset}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}"})
        try:
            with urllib.request.urlopen(req) as res:
                items = json.load(res).get("items", [])
                for item in items:
                    ident = item.get("identifier")
                    if ident: 
                        # 💡 修正：非表示化に必要な「商品名」や「在庫数」も一緒に記憶しておく
                        base_info[ident] = {
                            "price": item.get("price", 0),
                            "item_id": item.get("item_id"),
                            "title": item.get("title", ""),
                            "stock": item.get("stock", 0)
                        }
                if len(items) < 100: break
                offset += 100
        except: break
    return base_info

def hide_base_item(access_token, item_id, title, price, stock):
    url = "https://api.thebase.in/1/items/edit"
    # 💡 修正：必須項目（title, price, stock）を全て送ることでAPIの仕様変更（エラー弾き）に備える
    params = {
        "item_id": item_id, 
        "visible": 0,
        "title": title,
        "price": price,
        "stock": stock
    }
    req = urllib.request.Request(url, data=urlencode(params).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {access_token}")
    
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req) as res:
                return True
        except Exception as e:
            print(f"  ⚠️ BASE APIエラー (非表示化失敗 - 試行 {attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(3)
    
    return False

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
    print("🤖 ぽっけぇ～道 総合監視ロボ v11.7 起動...")
    print("🚀 [鉄壁のBASE非表示・API弾かれ対策版]")
    print("===========================================")
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
        client = gspread.authorize(creds)
        ss = client.open(SPREADSHEET_NAME)
        ws_set = ss.worksheet("システム設定")
        set_data = {str(r['Key']): str(r['Value']) for r in ws_set.get_all_records()}
        
        base_info = {}
        token = ""
        if set_data.get("CLIENT_ID") and set_data.get("BASE_REFRESH_TOKEN"):
            print("🔑 BASE APIトークンを更新・取得中...")
            token = refresh_base_token(ws_set, set_data["CLIENT_ID"], set_data["CLIENT_SECRET"], set_data["BASE_REFRESH_TOKEN"])
            if token: 
                base_info = get_base_items_info(token)
                print(f"✅ BASEの価格データを取得完了 ({len(base_info)}件)")

        db_sheet = ss.worksheet("在庫DB")
        log_sheet = ss.worksheet("価格ログ")
        records = db_sheet.get_all_records()
        header = db_sheet.row_values(1)
        
        url_col = header.index("スニダンURL") + 1
        base_p_col = header.index("BASE販売価格") + 1
        ref_p_col = header.index("参考相場") + 1
        
        targets = []
        for idx, row in enumerate(records):
            url = str(row.get('スニダンURL', '')).strip()
            if row.get('ステータス') != '売却済み' and url.startswith('http'):
                mode = "PSA10" if "10" in str(row.get('状態_PSA')) else "BOX"
                old_p_raw = row.get('参考相場')
                old_price = int(old_p_raw) if str(old_p_raw).isdigit() else 0
                
                ident = str(row.get('ID'))
                b_item = base_info.get(ident, {})
                
                targets.append({
                    "row_idx": idx + 2, "id": ident, "name": str(row.get('商品名')), 
                    "pack": str(row.get('収録パック')), "url": url, "mode": mode,
                    "base_price": int(b_item.get("price", 0)),
                    "base_item_id": b_item.get("item_id"),
                    "base_title": b_item.get("title", ""),
                    "base_stock": b_item.get("stock", 0),
                    "old_price": old_price
                })

        if not targets: 
            print("✅ 監視対象が見つかりません。終了します。")
            return
            
        print(f"🔍 監視対象を {len(targets)} 件発見しました。巡回を開始します...")
        send_discord(f"🔍 **総合監視開始** (対象: {len(targets)}件)")

        log_records_to_append = []
        update_cells = []
        consecutive_failures = 0

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            context = None
            page = None
            
            try:
                context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                context.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image", "media", "font"] else r.continue_())
                page = context.new_page()
                now = datetime.now()
                now_str = now.strftime("%Y/%m/%d %H:%M:%S")

                for i, t in enumerate(targets):
                    print(f"\n[{i+1}/{len(targets)}] ➡️ 調査({t['mode']}): {t['name']}")
                    
                    list_locator = None
                    for attempt in range(MAX_RETRIES):
                        try:
                            page.goto(t['url'], timeout=45000, wait_until="domcontentloaded")
                            page.evaluate("window.scrollBy(0, 800)")
                            time.sleep(1)
                            page.evaluate("window.scrollBy(0, 800)")
                            time.sleep(2)
                            list_locator = page.locator('ul.sales-history.item-list').first
                            list_locator.wait_for(state="attached", timeout=15000)
                            break
                        except:
                            if attempt < MAX_RETRIES - 1:
                                print(f"  ⚠️ ページ読み込み失敗 (試行 {attempt+1}/{MAX_RETRIES})")
                                time.sleep(5)
                            else:
                                print(f"  ❌ 最終的に取得失敗 → スキップします")

                    if not list_locator:
                        consecutive_failures += 1
                        if consecutive_failures >= CONSECUTIVE_FAILURES_LIMIT:
                            msg = "🚨 @everyone 【緊急停止】スニダンからアクセス拒否(BAN)された可能性があります。安全のためBOTを強制終了します。"
                            print(f"\n{msg}")
                            send_discord(msg)
                            break
                        continue
                    else:
                        consecutive_failures = 0

                    all_h = []
                    row_count = list_locator.locator('li').count()
                    
                    for j in range(row_count):
                        item = list_locator.locator('li').nth(j)
                        try:
                            date_text = item.locator('.date').text_content().strip()
                            size_text = item.locator('.size').text_content().strip()
                            price_text = item.locator('.price').text_content().strip()
                        except:
                            continue
                        
                        if j < 5:
                            print(f"    [🔍 生データ抽出] 日付:{date_text} | サイズ:{size_text} | 価格:{price_text}")
                            
                        if t['mode'] == "PSA10":
                            if not re.search(r'PSA\s*(?:10|１０)', size_text, re.IGNORECASE):
                                continue
                        else:
                            if not re.search(r'(?<!\d)1個(?!\d)|BOX|未開封', size_text, re.IGNORECASE):
                                continue
                        
                        price_match = re.search(r'¥([\d,]+)', price_text)
                        if price_match:
                            price = int(price_match.group(1).replace(",", ""))
                            dt = parse_snkrdunk_date(date_text, now)
                            if dt:
                                all_h.append({"date": dt, "price": price})

                    if not all_h: 
                        print("  💤 条件に一致する取引履歴が見つかりませんでした。")
                        continue

                    all_h.sort(key=lambda x: x['date'], reverse=True)
                    latest_10 = filter_abnormal_prices([x['price'] for x in all_h[:10]])
                    current_val = sum(latest_10) // len(latest_10) if latest_10 else all_h[0]['price']
                    
                    print(f"  📊 有効履歴 {len(all_h)} 件から最新相場を算出: ¥{current_val:,} (前回の記録: ¥{t['old_price']:,})")

                    # === 変動アラート ＆ ゆでガエル防止ロジック ===
                    diff = current_val - t['old_price']
                    trend = "安定"
                    
                    if t['old_price'] >= HIGH_PRICE_THRESHOLD:
                        threshold_val = HIGH_PRICE_FLUCTUATION
                    else:
                        threshold_val = int(t['old_price'] * CHANGE_NOTIFY_PERCENT)

                    is_market_alert = False
                    is_first_time = (t['old_price'] == 0)

                    if not is_first_time and abs(diff) >= threshold_val:
                        is_market_alert = True
                        if diff > 0:
                            trend = "上昇"
                            msg = f"📈 **【{t['mode']}高騰】** {t['name']}\n前回: ¥{t['old_price']:,} ➡️ **最新: ¥{current_val:,}** (+¥{diff:,})\n🔗 {t['url']}"
                            send_discord(msg)
                        else:
                            trend = "下降"
                            msg = f"📉 **【{t['mode']}暴落】** {t['name']}\n前回: ¥{t['old_price']:,} ➡️ **最新: ¥{current_val:,}** (-¥{abs(diff):,})\n🔗 {t['url']}"
                            send_discord(msg)
                    else:
                        if diff > 0: trend = "上昇"
                        elif diff < 0: trend = "下降"

                    # === 💡 2段階防衛：価格接近アラート＆自動非表示 ===
                    if t['base_price'] > 0 and t['base_item_id']:
                        ratio = current_val / t['base_price']
                        
                        if ratio >= BASE_PRICE_PROXIMITY_HIDE:
                            print(f"  🚨 緊急停止: スニダン相場がBASE価格の95%に達しました。自動非表示を実行します。")
                            # 💡 修正：名前や在庫情報も一緒に渡して完璧な命令を作成
                            success = hide_base_item(token, t['base_item_id'], t['base_title'], t['base_price'], t['base_stock'])
                            if success:
                                msg = f"🚨 **【緊急停止・自動非表示化 完了】** {t['name']}\nスニダン相場(¥{current_val:,})がBASE価格(¥{t['base_price']:,})の95%以上に達したため、**BASEでの出品を自動的に「非公開」に変更して保護しました。**\n🔗 {t['url']}"
                            else:
                                msg = f"🚨 @everyone **【致命的エラー・自動非表示化 失敗】** {t['name']}\n相場が95%以上に達しましたが、BASEの通信エラーにより商品の取り下げに失敗しました！\n**至急、手動でBASEを非公開にしてください！**\n🔗 {t['url']}"
                            send_discord(msg)
                            
                        elif ratio >= BASE_PRICE_PROXIMITY_ALERT:
                            print(f"  ⚠️ 接近検知: スニダン相場がBASE価格の90%に達しました(通知のみ)。")
                            msg = f"⚠️ **【価格接近アラート】** {t['name']}\nBASE販売価格: ¥{t['base_price']:,}に対し、\n**スニダン相場が ¥{current_val:,} まで上昇しています** (90%超)\n🔗 {t['url']}"
                            send_discord(msg)

                    log_records_to_append.append([
                        now_str, t['id'], t['name'], t['base_price'], current_val, trend, t['url']
                    ])
                    
                    if is_first_time or is_market_alert:
                        sync_count = 0
                        for idx, row in enumerate(records):
                            if str(row.get('商品名')) == t['name'] and str(row.get('収録パック')) == t['pack']:
                                r_idx = idx + 2
                                update_cells.append(gspread.Cell(row=r_idx, col=ref_p_col, value=current_val))
                                item_id_str = str(row.get('ID'))
                                if item_id_str in base_info:
                                    update_cells.append(gspread.Cell(row=r_idx, col=base_p_col, value=base_info[item_id_str]["price"]))
                                sync_count += 1
                        print(f"  🔄 在庫DB {sync_count} 行分の同期データをセットしました。")
                    else:
                        print(f"  💤 基準相場維持のため、在庫DBの更新は行いません。")
                    
                    time.sleep(random.uniform(8, 15))
                    
            finally:
                print("🧹 メモリ解放・ブラウザ完全終了処理を実行します...")
                try:
                    if page: page.close()
                    if context: context.close()
                    if browser: browser.close()
                except Exception as e:
                    pass
            
            print("\n===========================================")
            print("💾 最終データ書き込みフェーズ")
            if update_cells:
                db_sheet.update_cells(update_cells)
                print(f"✅ 在庫DBを一括更新しました ({len(update_cells)//2}箇所)")
            if log_records_to_append:
                log_sheet.append_rows(log_records_to_append)
                print(f"✅ 価格ログシートに {len(log_records_to_append)} 件の記録を追加しました。")
            send_discord("✅ **総合監視完了**")
            print("🏁 全巡回完了")
            print("===========================================")
            
    except Exception:
        traceback.print_exc()

if __name__ == "__main__":
    run_robot()
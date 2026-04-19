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
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# ---------------------------------------------------------
# ⚙️ 設定・定数 (v4.4)
# ---------------------------------------------------------
JSON_KEY_FILE = 'secrets.json'
SPREADSHEET_NAME = 'ぽっけぇ〜道_システムv3'

SHEET_INVENTORY = '在庫DB'
SHEET_PURCHASE = '仕入帳'
SHEET_SALES = '売上帳'

# ---------------------------------------------------------
# 🔌 データベース接続＆初期化機能
# ---------------------------------------------------------
@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = st.secrets["gcp_service_account"]
        elif "private_key" in st.secrets:
            key_dict = st.secrets
        elif os.path.exists(JSON_KEY_FILE):
            creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
            return gspread.authorize(creds)
        else:
            return None
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        return gspread.authorize(creds)
    except Exception:
        return None

def get_spreadsheet():
    client = get_gspread_client()
    if client:
        try:
            return client.open(SPREADSHEET_NAME)
        except gspread.exceptions.SpreadsheetNotFound:
            return None
    return None

def check_and_init_sheets():
    sh = get_spreadsheet()
    if not sh: return None, None, None

    try:
        ws_inv = sh.worksheet(SHEET_INVENTORY)
    except:
        ws_inv = sh.add_worksheet(title=SHEET_INVENTORY, rows=1000, cols=17)
        ws_inv.append_row(['ID', '商品名', '収録パック', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号', '相場更新'])

    try:
        ws_pur = sh.worksheet(SHEET_PURCHASE)
    except:
        ws_pur = sh.add_worksheet(title=SHEET_PURCHASE, rows=1000, cols=14)
        ws_pur.append_row(['ID', '仕入日', '仕入名目', '商品名', '収録パック', '種類', '状態_PSA', '数量', '単価', '小計', '仕入先', '備考', '登録日時'])

    try:
        ws_sales = sh.worksheet(SHEET_SALES)
    except:
        ws_sales = sh.add_worksheet(title=SHEET_SALES, rows=1000, cols=15)
        ws_sales.append_row(['ID', '元の在庫ID', '売却日', '商品名', '収録パック', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時'])

    return ws_inv, ws_pur, ws_sales

@st.cache_data(ttl=60)
def load_data():
    ws_inv, _, _ = check_and_init_sheets()
    if ws_inv:
        try:
            df = get_as_dataframe(ws_inv, evaluate_formulas=True)
            df = df.dropna(subset=['ID'])
            df = df[df['ID'] != '']
            if 'PSA番号' not in df.columns: df['PSA番号'] = ""
            if '収録パック' not in df.columns: df['収録パック'] = ""
            if '状態_PSA' not in df.columns: df['状態_PSA'] = "A (美品)"
            if '相場更新' not in df.columns: 
                df['相場更新'] = True
            else:
                df['相場更新'] = df['相場更新'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False, '1': True, '0': False})
                df['相場更新'] = df['相場更新'].fillna(True).astype(bool)

            df['原価'] = pd.to_numeric(df['原価'], errors='coerce').fillna(0).astype(int)
            df['参考相場'] = pd.to_numeric(df['参考相場'], errors='coerce').fillna(0).astype(int)
            df['在庫数'] = pd.to_numeric(df['在庫数'], errors='coerce').fillna(0).astype(int)
            df['PSA番号'] = df['PSA番号'].astype(str).replace('nan', '')
            df['収録パック'] = df['収録パック'].astype(str).replace('nan', '')
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    ws_inv, _, _ = check_and_init_sheets()
    if ws_inv:
        save_cols = ['ID', '商品名', '収録パック', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号', '相場更新']
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns:
                if col == '相場更新': df_to_save[col] = True
                else: df_to_save[col] = ""
        df_to_save = df_to_save[save_cols]
        ws_inv.clear()
        set_with_dataframe(ws_inv, df_to_save)
        load_data.clear()

@st.cache_data(ttl=60)
def load_sales_data():
    _, _, ws_sales = check_and_init_sheets()
    if ws_sales:
        try:
            df = get_as_dataframe(ws_sales, evaluate_formulas=True)
            df = df.dropna(subset=['ID'])
            df = df[df['ID'] != '']
            if '元の在庫ID' not in df.columns: df['元の在庫ID'] = ""
            if '収録パック' not in df.columns: df['収録パック'] = ""
            if '状態_PSA' not in df.columns:
                df['状態_PSA'] = df['商品名'].astype(str).apply(lambda x: '-' if 'オリパ' in x or 'サプライ' in x else 'A (美品)')
            for col in ['売却数', '売上額', '手数料', '経費_送料', '純利益']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_sales_data(df):
    _, _, ws_sales = check_and_init_sheets()
    if ws_sales:
        save_cols = ['ID', '元の在庫ID', '売却日', '商品名', '収録パック', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時']
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns:
                df_to_save[col] = ""
        df_to_save = df_to_save[save_cols]
        ws_sales.clear()
        set_with_dataframe(ws_sales, df_to_save)
        load_sales_data.clear()

@st.cache_data(ttl=60)
def load_purchase_data():
    _, ws_pur, _ = check_and_init_sheets()
    if ws_pur:
        try:
            df = get_as_dataframe(ws_pur, evaluate_formulas=True)
            df = df.dropna(subset=['ID'])
            df = df[df['ID'] != '']
            if '収録パック' not in df.columns: df['収録パック'] = ""
            if '状態_PSA' not in df.columns:
                if '種類' in df.columns:
                    df['状態_PSA'] = df['種類'].apply(lambda x: '-' if x in ['オリジナルパック', 'サプライ'] else 'A (美品)')
                else:
                    df['状態_PSA'] = 'A (美品)'
            for col in ['数量', '単価', '小計']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_purchase_data(df):
    _, ws_pur, _ = check_and_init_sheets()
    if ws_pur:
        save_cols = ['ID', '仕入日', '仕入名目', '商品名', '収録パック', '種類', '状態_PSA', '数量', '単価', '小計', '仕入先', '備考', '登録日時']
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns:
                df_to_save[col] = ""
        df_to_save = df_to_save[save_cols]
        ws_pur.clear()
        set_with_dataframe(ws_pur, df_to_save)
        load_purchase_data.clear()

def record_purchase_items(batch_id, date, title, source, note, items):
    _, ws_pur, _ = check_and_init_sheets()
    if ws_pur:
        rows = []
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for item in items:
            row = [
                f"{batch_id}-{str(uuid.uuid4())[:4]}",
                date, title, item['name'], item.get('pack', ''), item['type'], item.get('cond', 'A (美品)'),
                item['qty'], item['unit_cost'], item['subtotal'], source, note, now_str
            ]
            rows.append(row)
        if rows:
            ws_pur.append_rows(rows)
            load_purchase_data.clear()

def recalculate_moving_average_costs():
    df_inv = load_data()
    df_pur = load_purchase_data()
    df_sales = load_sales_data()
    
    if df_inv.empty or df_pur.empty: return df_inv
    
    history = {}
    events = []
    
    for _, row in df_pur.iterrows():
        events.append({
            'time': row['登録日時'], 'type': 'pur', 
            'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(),
            'cond': str(row.get('状態_PSA', 'A (美品)')).strip(),
            'qty': int(row['数量']), 'subtotal': int(row['小計'])
        })
    for _, row in df_sales.iterrows():
        events.append({
            'time': row['登録日時'], 'type': 'sale', 
            'name': str(row['商品名']).strip(), 'pack': str(row.get('収録パック', '')).strip(),
            'cond': str(row.get('状態_PSA', 'A (美品)')).strip(),
            'qty': int(row['売却数'])
        })
        
    events.sort(key=lambda x: x['time'])
    
    for ev in events:
        key = (ev['name'], ev['pack'], ev['cond'])
        if key not in history:
            history[key] = {'qty': 0, 'cost': 0}
            
        state = history[key]
        if ev['type'] == 'pur':
            new_qty = state['qty'] + ev['qty']
            total_val = (state['qty'] * state['cost']) + ev['subtotal']
            state['cost'] = int(total_val / new_qty) if new_qty > 0 else 0
            state['qty'] = new_qty
        elif ev['type'] == 'sale':
            state['qty'] -= ev['qty']
            if state['qty'] < 0: state['qty'] = 0
            
    for idx, row in df_inv.iterrows():
        key = (str(row['商品名']).strip(), str(row.get('収録パック', '')).strip(), str(row.get('状態_PSA', 'A (美品)')).strip())
        if key in history:
            df_inv.at[idx, '原価'] = history[key]['cost']
            
    return df_inv

# ---------------------------------------------------------
# 🌐 スクレイピング＆文字列クリーニング (v4.3/v4.4 トリプルチェック搭載)
# ---------------------------------------------------------
def clean_product_name(text):
    if not isinstance(text, str): return str(text)
    text = re.sub(r'\{-}.*$', '', text)
    return text.strip()

def generate_search_keyword(orig_name):
    is_box = "BOX" in orig_name.upper() or "ｂｏｘ" in orig_name.lower()
    
    match = re.search(r'『(.+?)』', orig_name)
    if match:
        base = match.group(1)
    else:
        base = orig_name
        
    cleaned = re.sub(r'\[.*?\]|\(.*?\)|【.*?】', '', base).strip()
    cleaned = cleaned.replace("拡張パック", "").replace("強化", "").replace("ハイクラスパック", "").replace("構築済みデッキ", "").strip()
    
    if is_box and "BOX" not in cleaned.upper():
        cleaned += " BOX"
        
    if not cleaned: cleaned = orig_name
    return cleaned.strip()

def get_best_match(orig_name, orig_pack, results):
    ng_words = ["キズ", "傷", "イタミ", "ダメージ", "シュリンクなし", "シュリンク破れ", "特価", "難あり", "訳あり", "ジャンク", "開封済"]
    special_words = ["デラックス", "スペシャル", "プレミアム", "セット", "ジャンボ", "コレクション", "クラシック"]
    
    for res in results:
        res_name = res['name']
        res_pack = res['pack']
        
        if any(ng in res_name for ng in ng_words):
            continue
            
        if orig_pack and res_pack:
            if orig_pack.upper() != res_pack.upper():
                continue
        
        is_safe = True
        for sw in special_words:
            if sw not in orig_name and sw in res_name:
                is_safe = False
                break
        if not is_safe:
            continue
            
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
            if pack_match:
                pack_code = pack_match.group(1)
            
            is_box = "BOX" in raw_name.upper() or "ｂｏｘ" in raw_name.lower()
            clean_name = clean_product_name(raw_name)
            if is_box and "BOX" not in clean_name.upper():
                clean_name = f"{clean_name} BOX"
            
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
                    if attr in img.attrs and img[attr]:
                        temp_url = img[attr]
                        break
                if temp_url:
                    t_lower = temp_url.lower()
                    if any(bad_word in t_lower for bad_word in ["spacer", "blank", "icon", "ranking", "mark", "sold"]): continue
                    img_url = temp_url
                    break
                            
            if img_url.startswith('/'): img_url = "https://www.cardrush-pokemon.jp" + img_url

            product_url = ""
            a_tag = item.select_one('a[href]')
            if a_tag:
                product_url = a_tag['href']
                if product_url.startswith('/'):
                    product_url = "https://www.cardrush-pokemon.jp" + product_url

            if price > 0:
                results.append({
                    "name": clean_name, "pack": pack_code, "price": price, 
                    "image": img_url, "url": product_url
                })
        
        unique_results = []
        seen_urls = set()
        for r in results:
            identifier = r['url'] if r['url'] else r['name']
            if identifier not in seen_urls:
                unique_results.append(r)
                seen_urls.add(identifier)
                
        return unique_results
    except Exception:
        return []

def search_card_rush(keyword):
    base_url = "https://www.cardrush-pokemon.jp"
    encoded_keyword = quote(keyword.encode('utf-8'))
    url_a = f"{base_url}/product-list?keyword={encoded_keyword}&num=50"
    results = fetch_from_url(url_a)
    if not results:
        url_b = f"{base_url}/shop/shopbrand.html?search={encoded_keyword}"
        results = fetch_from_url(url_b)
    return results

# ---------------------------------------------------------
# 🖥️ アプリ画面 (v4.4)
# ---------------------------------------------------------
st.set_page_config(page_title="ぽっけぇ～道 システム", layout="wide")
st.title("🎴 ぽっけぇ～道 管理システム v4.4")

if 'cart' not in st.session_state: st.session_state['cart'] = []
if 'has_searched' not in st.session_state: st.session_state['has_searched'] = False
if 'reset_key' not in st.session_state: st.session_state['reset_key'] = 0

menu = st.sidebar.radio(
    "【作業メニュー】", 
    ["📦 スピード仕入・解体", "📊 在庫・PSA管理", "🛍️ オリパ工場", "📖 帳簿・分析"]
)

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
            search_word = st.text_input("カード名・BOX名を入力", placeholder="例: クレイバースト BOX, ナンジャモ SAR")
            if st.button("検索", type="primary", use_container_width=True):
                if search_word:
                    with st.spinner("検索中..."):
                        st.session_state['search_res'] = search_card_rush(search_word)
                        st.session_state['has_searched'] = True
                else:
                    st.warning("キーワードを入力してください。")
            
            if st.session_state.get('has_searched'):
                if st.session_state.get('search_res'):
                    sort_order = st.selectbox("並び替え", ["おすすめ順", "価格の高い順", "価格の安い順"])
                    
                    display_res = list(st.session_state['search_res'])
                    if sort_order == "価格の高い順": display_res.sort(key=lambda x: x['price'], reverse=True)
                    elif sort_order == "価格の安い順": display_res.sort(key=lambda x: x['price'])

                    st.write("---")
                    for item in display_res:
                        c1, c2, c3 = st.columns([1, 3, 2])
                        with c1:
                            if item['image']: st.image(item['image'], width=50)
                            else: st.write("🖼️ 画像なし")
                        with c2:
                            disp_pack = f" [{item['pack']}]" if item['pack'] else ""
                            safe_name = item['name'].replace('<', '&lt;').replace('>', '&gt;')
                            if item.get('url'):
                                st.markdown(f'<a href="{item["url"]}" target="_blank" style="font-weight: bold; color: #1f77b4; text-decoration: none;">{safe_name}{disp_pack}</a>', unsafe_allow_html=True)
                            else:
                                st.write(f"**{item['name']}{disp_pack}**")
                            st.caption(f"相場: ¥{item['price']:,}")
                        with c3:
                            with st.popover("カートに追加"):
                                unique_key = f"{item.get('url', str(uuid.uuid4()))}_{item['name']}"
                                add_qty = st.number_input("枚数/個数", min_value=1, value=1, key=f"qty_{unique_key}")
                                add_cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封"], key=f"cond_{unique_key}")
                                if st.button("追加する", key=f"add_{unique_key}", use_container_width=True):
                                    st.session_state['cart'].append({
                                        "id": str(uuid.uuid4())[:8], 
                                        "name": item['name'], "pack": item['pack'],
                                        "type": "未開封BOX" if "BOX" in item['name'].upper() else "シングルカード",
                                        "cond": add_cond, "qty": add_qty, "market_price": item['price'],
                                        "auto_update": True
                                    })
                                    st.success("追加しました！")
                                    time.sleep(0.5); st.rerun()
                    st.write("---")
                else:
                    st.error("見つかりませんでした。別のキーワードで試すか、「手動登録」タブから追加してください。")
        
        with tab_manual:
            st.info("検索で見つからないマイナーな商品や、エラーカードなどを手動でカートに入れます。")
            man_name = st.text_input("商品名 (必須)", placeholder="例: 限定プロモカード")
            man_pack = st.text_input("収録パック略号 (任意)", placeholder="例: M2a, SV4a")
            c_type, c_cond = st.columns(2)
            with c_type: man_type = st.selectbox("種類", ["シングルカード", "未開封BOX", "未開封パック", "その他"])
            with c_cond: man_cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封", "-"])
            c_price, c_qty = st.columns(2)
            with c_price: man_price = st.number_input("1個あたりの参考相場 (円)", min_value=0, step=100)
            with c_qty: man_qty = st.number_input("数量", min_value=1, value=1)
            man_auto = st.checkbox("この商品の相場を今後自動更新する", value=False, help="チェックを入れると、メンテナンス画面での相場一括更新の対象になります。")
            
            if st.button("✍️ 手動でカートに追加", use_container_width=True):
                if man_name:
                    st.session_state['cart'].append({
                        "id": str(uuid.uuid4())[:8], "name": man_name, "pack": man_pack, "type": man_type,
                        "cond": man_cond, "qty": man_qty, "market_price": man_price,
                        "auto_update": man_auto
                    })
                    st.success(f"「{man_name}」をカートに追加しました！"); time.sleep(0.5); st.rerun()
                else:
                    st.warning("商品名を入力してください。")
        
        with tab_bulk:
            bulk_type = st.selectbox("素材の種類", ["【素材】SR", "【素材】AR", "【素材】RR", "【素材】CHR", "【素材】K", "【素材】汎用ノーマル"])
            bulk_price = st.number_input("1枚あたりの相場（価値）", min_value=0, value=30, step=10)
            bulk_qty = st.number_input("枚数", min_value=1, value=100, step=10)
            if st.button("素材をカートに追加", use_container_width=True):
                st.session_state['cart'].append({
                    "id": str(uuid.uuid4())[:8], "name": bulk_type, "pack": "", "type": "素材・バルク",
                    "cond": "プレイ用", "qty": bulk_qty, "market_price": bulk_price, "auto_update": False
                })
                st.success("追加しました！"); st.rerun()

        with tab_supply:
            sup_name = st.text_input("品名", placeholder="例: 100均スリーブ")
            sup_qty = st.number_input("個数", min_value=1, value=1)
            sup_price = st.number_input("1個あたりの金額", min_value=0, step=100)
            if st.button("サプライをカートに追加", use_container_width=True):
                if sup_name:
                    st.session_state['cart'].append({
                        "id": str(uuid.uuid4())[:8], "name": f"【サプライ】{sup_name}", "pack": "", "type": "サプライ",
                        "cond": "-", "qty": sup_qty, "market_price": sup_price, "auto_update": False
                    })
                    st.success("追加しました！"); st.rerun()

    with col_right:
        st.subheader("② カートの中身と原価計算")
        with st.container(border=True):
            rk = st.session_state['reset_key']
            total_paid = st.number_input("支払った総額 (送料・手数料込み)", min_value=0, step=1000, key=f"total_paid_{rk}")
            purchase_title = st.text_input("仕入名目 (任意)", placeholder="例: 秋葉原福袋", key=f"purchase_title_{rk}")
            purchase_source = st.selectbox("仕入先", ["店舗", "フリマ(メルカリ等)", "オンラインオリパ", "問屋", "自己所有・過去の在庫", "その他"], key=f"purchase_source_{rk}")
            
        if not st.session_state['cart']:
            st.caption("カートは空です。")
        else:
            total_market_value = sum(item['qty'] * item['market_price'] for item in st.session_state['cart'])
            calculated_cart = []
            for item in st.session_state['cart']:
                item_total_market = item['qty'] * item['market_price']
                if total_market_value > 0:
                    ratio = item_total_market / total_market_value
                    unit_cost = int((total_paid * ratio) / item['qty'])
                else:
                    unit_cost = 0
                calculated_cart.append({
                    "削除": False, "ID": item['id'], "商品名": item['name'], "収録パック": item.get('pack', ''),
                    "状態": item['cond'],
                    "種類": item['type'], "数量": item['qty'],
                    "自動計算原価": unit_cost, "参考相場": item['market_price']
                })
            
            calc_df = pd.DataFrame(calculated_cart)
            st.write(f"💡 カート内の相場合計: **¥{total_market_value:,}**")
            
            edited_cart = st.data_editor(
                calc_df, hide_index=True,
                column_config={
                    "削除": st.column_config.CheckboxColumn("削除", default=False), 
                    "ID": None,
                    "数量": st.column_config.NumberColumn("数量", min_value=1, step=1),
                    "収録パック": st.column_config.TextColumn("収録パック"),
                    "状態": st.column_config.TextColumn("状態", disabled=True),
                    "商品名": st.column_config.TextColumn("商品名", disabled=True),
                    "種類": st.column_config.TextColumn("種類", disabled=True),
                    "自動計算原価": st.column_config.NumberColumn("自動計算原価", disabled=True, format="¥%d"),
                    "参考相場": st.column_config.NumberColumn("参考相場", disabled=True, format="¥%d")
                },
                use_container_width=True
            )
            
            needs_rerun = False
            for idx, row in edited_cart.iterrows():
                item_id = row['ID']
                new_qty = row['数量']
                new_pack = row['収録パック']
                for s_item in st.session_state['cart']:
                    if s_item['id'] == item_id:
                        if s_item['qty'] != new_qty:
                            s_item['qty'] = new_qty
                            needs_rerun = True
                        if s_item.get('pack', '') != new_pack:
                            s_item['pack'] = new_pack
                            needs_rerun = True
            if needs_rerun:
                st.rerun()
            
            if edited_cart['削除'].any():
                if st.button("🗑️ チェックした商品を外す"):
                    ids_to_keep = edited_cart[~edited_cart['削除']]['ID'].tolist()
                    st.session_state['cart'] = [item for item in st.session_state['cart'] if item['id'] in ids_to_keep]
                    st.rerun()

            st.divider()
            if st.button("✨ この内容で在庫DBと帳簿に一括登録 ✨", type="primary", use_container_width=True):
                df_inv = load_data()
                batch_id = "B" + str(uuid.uuid4())[:7]
                purchase_date = datetime.now().strftime('%Y-%m-%d')
                
                new_inventory_rows = []
                purchase_items_for_log = []
                
                for idx, row in edited_cart.iterrows():
                    item_id = row['ID']
                    original_item = next(item for item in st.session_state['cart'] if item['id'] == item_id)
                    item_type = row['種類']
                    item_name = row['商品名']
                    item_pack = row['収録パック']
                    new_qty = int(row['数量'])
                    new_cost = int(row['自動計算原価'])
                    
                    purchase_items_for_log.append({
                        'name': item_name, 'pack': item_pack, 'type': item_type, 'cond': original_item['cond'],
                        'qty': new_qty, 'unit_cost': new_cost, 'subtotal': new_qty * new_cost
                    })
                    
                    if original_item['type'] != "サプライ":
                        is_merged = False
                        if item_type in ["未開封BOX", "素材・バルク"] and not df_inv.empty:
                            # ✨ v4.4 修正点: 同一商品の合流に「状態」の条件を追加
                            mask_name = df_inv['商品名'].astype(str).str.strip() == str(item_name).strip()
                            mask_type = df_inv['種類'].astype(str).str.strip() == str(item_type).strip()
                            mask_pack = df_inv['収録パック'].astype(str).str.strip() == str(item_pack).strip()
                            mask_cond = df_inv['状態_PSA'].astype(str).str.strip() == str(original_item['cond']).strip()
                            
                            match_idx = df_inv[mask_name & mask_type & mask_pack & mask_cond].index
                            if len(match_idx) > 0:
                                target_idx = match_idx[0]
                                current_qty = int(df_inv.at[target_idx, '在庫数'])
                                current_cost = int(df_inv.at[target_idx, '原価'])
                                
                                total_value = (current_qty * current_cost) + (new_qty * new_cost)
                                new_total_qty = current_qty + new_qty
                                new_avg_cost = int(total_value / new_total_qty) if new_total_qty > 0 else 0
                                
                                df_inv.at[target_idx, '在庫数'] = new_total_qty
                                df_inv.at[target_idx, '原価'] = new_avg_cost
                                df_inv.at[target_idx, '仕入日'] = purchase_date
                                is_merged = True
                        
                        if not is_merged:
                            new_inventory_rows.append({
                                'ID': item_id, '商品名': item_name, '収録パック': item_pack, '種類': item_type,
                                '状態_PSA': original_item['cond'], '仕入日': purchase_date,
                                '原価': new_cost, '参考相場': row['参考相場'],
                                '在庫数': new_qty, '仕入元': purchase_source,
                                'ステータス': '在庫あり', 'PSA番号': '', '相場更新': original_item.get('auto_update', True)
                            })
                
                if new_inventory_rows:
                    new_inv_df = pd.DataFrame(new_inventory_rows)
                    df_inv = pd.concat([df_inv, new_inv_df], ignore_index=True) if not df_inv.empty else new_inv_df
                
                save_data(df_inv)
                record_title = purchase_title if purchase_title else "一括仕入"
                record_purchase_items(batch_id, purchase_date, record_title, purchase_source, "カート一括登録", purchase_items_for_log)
                
                st.session_state['cart'] = []
                st.session_state['has_searched'] = False
                st.session_state['reset_key'] += 1

                st.success("🎉 在庫DBおよび仕入帳（明細）に登録しました！")
                time.sleep(1.5); st.rerun()

# =========================================================
# 📊 第2フェーズ：在庫・PSA管理 ＋ 🛒売却レジ
# =========================================================
elif menu == "📊 在庫・PSA管理":
    st.header("📊 在庫・PSA管理")
    df = load_data()
    
    if df.empty:
        st.info("現在、データベースに在庫がありません。「スピード仕入」から商品を登録してください。")
    else:
        df_active = df[df['ステータス'] != '売却済み'].copy()
        
        tab_singles, tab_box_bulk, tab_psa, tab_sell, tab_edit, tab_maintenance = st.tabs([
            "🃏 シングル在庫", "📦 BOX・素材", "💎 PSA管理", "🛒 売却レジ", "✏️ データ編集", "🛠️ メンテナンス"
        ])
        
        with tab_singles:
            st.subheader("🃏 シングルカード在庫 (PSA以外)")
            df_single = df_active[(df_active['種類'] == 'シングルカード') & (~df_active['ステータス'].isin(['PSA提出中', '鑑定済み']))]
            if not df_single.empty:
                st.dataframe(df_single[['商品名', '収録パック', '状態_PSA', '原価', '参考相場', '在庫数', '仕入日', 'ステータス']], use_container_width=True, hide_index=True)
                st.divider()
                single_options = {f"[{row['収録パック']}] {row['商品名']} (ID: {row['ID']})": row['ID'] for idx, row in df_single.iterrows()}
                target_to_psa = st.selectbox("提出するカードを選択してください", options=list(single_options.keys()), index=None)
                if target_to_psa and st.button("✈️ 「PSA提出中」にする", type="primary"):
                    df.loc[df['ID'] == single_options[target_to_psa], 'ステータス'] = 'PSA提出中'
                    save_data(df)
                    st.success("変更しました！"); time.sleep(1); st.rerun()
            else:
                st.caption("現在、該当する在庫はありません。")

        with tab_box_bulk:
            st.subheader("📦 未開封BOX・素材")
            df_bb = df_active[df_active['種類'].isin(['未開封BOX', '素材・バルク', 'オリジナルパック', '未開封パック'])].copy()
            if not df_bb.empty:
                st.dataframe(df_bb[['商品名', '収録パック', '種類', '原価', '在庫数', '参考相場', 'ステータス']], use_container_width=True, hide_index=True)
            else:
                st.caption("現在、該当する在庫はありません。")

        with tab_psa:
            st.subheader("💎 PSA管理 (提出中・鑑定済み)")
            df_psa_pending = df_active[df_active['ステータス'] == 'PSA提出中']
            df_psa_done = df_active[df_active['ステータス'] == '鑑定済み']
            
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("##### ⏳ PSA提出中")
                if not df_psa_pending.empty: st.dataframe(df_psa_pending[['商品名', '収録パック', '原価', '仕入日']], hide_index=True)
                else: st.caption("現在、提出中のカードはありません。")
            with c2:
                st.markdown("##### ✨ 鑑定済み (ストック)")
                if not df_psa_done.empty: st.dataframe(df_psa_done[['商品名', '収録パック', '状態_PSA', 'PSA番号', '原価']], hide_index=True)
                else: st.caption("現在、鑑定済みのカードはありません。")

            if not df_psa_pending.empty:
                st.divider()
                st.markdown("##### 📥 鑑定結果の登録（原価への費用加算）")
                psa_opts = {f"[{row['収録パック']}] {row['商品名']} (ID: {row['ID']})": row['ID'] for idx, row in df_psa_pending.iterrows()}
                with st.form("psa_return_form"):
                    target_ret = st.selectbox("戻ってきたカード", options=list(psa_opts.keys()))
                    cc1, cc2, cc3 = st.columns(3)
                    with cc1: grade = st.selectbox("鑑定結果", ["10", "9", "8", "7以下"])
                    with cc2: cert = st.text_input("PSA番号", placeholder="12345678")
                    with cc3: fee = st.number_input("鑑定費用 (円)", min_value=0, value=3300, step=100)
                    
                    if st.form_submit_button("結果を登録して原価を更新", type="primary"):
                        target_id = psa_opts[target_ret]
                        df.loc[df['ID'] == target_id, '原価'] += fee
                        df.loc[df['ID'] == target_id, 'ステータス'] = '鑑定済み'
                        df.loc[df['ID'] == target_id, '状態_PSA'] = f"PSA {grade}"
                        df.loc[df['ID'] == target_id, 'PSA番号'] = cert
                        save_data(df)
                        st.success("登録しました！"); time.sleep(1); st.rerun()

            st.divider()
            st.markdown("##### ⚙️ その他のPSA関連操作")
            c_cancel, c_crack = st.columns(2)
            
            with c_cancel:
                st.markdown("**❌ 提出のキャンセル**")
                if not df_psa_pending.empty:
                    cancel_opts = {f"[{row['収録パック']}] {row['商品名']} [ID:{row['ID']}]": row['ID'] for idx, row in df_psa_pending.iterrows()}
                    target_cancel = st.selectbox("キャンセルするカード", options=list(cancel_opts.keys()), key="cancel_psa")
                    if target_cancel and st.button("通常在庫に戻す", key="btn_cancel"):
                        df.loc[df['ID'] == cancel_opts[target_cancel], 'ステータス'] = '在庫あり'
                        save_data(df)
                        st.success("提出をキャンセルし、通常在庫に戻しました！")
                        time.sleep(1); st.rerun()
                else:
                    st.caption("キャンセル可能なカードはありません。")
                    
            with c_crack:
                st.markdown("**🔨 ケース割り (鑑定済み→通常在庫)**")
                st.caption("※かかった鑑定費用(原価)はそのまま引き継がれます。")
                if not df_psa_done.empty:
                    crack_opts = {f"[{row['収録パック']}] {row['商品名']} ({row['状態_PSA']}) [ID:{row['ID']}]": row['ID'] for idx, row in df_psa_done.iterrows()}
                    target_crack = st.selectbox("割るカードを選択", options=list(crack_opts.keys()), key="crack_psa")
                    if target_crack and st.button("ケースを割って通常在庫へ", key="btn_crack"):
                        crack_id = crack_opts[target_crack]
                        df.loc[df['ID'] == crack_id, 'ステータス'] = '在庫あり'
                        df.loc[df['ID'] == crack_id, '状態_PSA'] = 'A (美品)'
                        df.loc[df['ID'] == crack_id, 'PSA番号'] = ''
                        save_data(df)
                        st.success("通常在庫に戻しました！")
                        time.sleep(1.5); st.rerun()
                else:
                    st.caption("割れるカードはありません。")

        with tab_sell:
            st.subheader("🛒 売却レジ (レジ打ち)")
            st.write("在庫から商品を売却し、手数料を自動計算して帳簿に記録します。")
            
            sell_options = {f"[{row['収録パック']}] {row['商品名']} (残:{row['在庫数']} | 原価:¥{row['原価']}) [ID:{row['ID']}]": row['ID'] for idx, row in df_active[df_active['在庫数'] > 0].iterrows()}
            target_sell = st.selectbox("売却する商品を選択してください", options=list(sell_options.keys()), index=None)

            if target_sell:
                item_id = sell_options[target_sell]
                item_row = df_active[df_active['ID'] == item_id].iloc[0]
                
                with st.form("sell_form"):
                    c1, c2 = st.columns(2)
                    with c1:
                        sell_qty = st.number_input("売却数", min_value=1, max_value=int(item_row['在庫数']), value=1)
                        sell_price = st.number_input("売上総額 (お客様が支払った金額)", min_value=0, step=100)
                    with c2:
                        channel = st.selectbox("販路", ["BASE (Web経由)", "BASE (PayIDアプリ経由)", "メルカリ", "店舗・直接取引", "その他"])
                        shipping_cost = st.number_input("実際の送料・梱包費 (経費)", min_value=0, step=10, value=185)
                        
                    sell_date = st.date_input("売却日", datetime.now())
                    note = st.text_input("備考", placeholder="購入者名など")
                    
                    submitted = st.form_submit_button("売却を確定して帳簿に記録", type="primary", use_container_width=True)
                    
                    if submitted:
                        if channel == "BASE (Web経由)": fee = int(sell_price * 0.066) + 40 if sell_price > 0 else 0
                        elif channel == "BASE (PayIDアプリ経由)": fee = int(sell_price * 0.095) + 40 if sell_price > 0 else 0
                        elif channel == "メルカリ": fee = int(sell_price * 0.10)
                        else: fee = 0
                            
                        total_cost = item_row['原価'] * sell_qty
                        profit = sell_price - fee - shipping_cost - total_cost
                        
                        new_qty = item_row['在庫数'] - sell_qty
                        if new_qty <= 0:
                            df.loc[df['ID'] == item_id, '在庫数'] = 0
                            df.loc[df['ID'] == item_id, 'ステータス'] = '売却済み'
                        else:
                            df.loc[df['ID'] == item_id, '在庫数'] = new_qty
                        save_data(df)
                        
                        df_sales = load_sales_data()
                        sale_id = "S" + str(uuid.uuid4())[:7]
                        new_sale = pd.DataFrame([{
                            'ID': sale_id, '元の在庫ID': item_id, '売却日': str(sell_date), '商品名': item_row['商品名'], '収録パック': item_row['収録パック'],
                            '状態_PSA': item_row['状態_PSA'], # ✨ v4.4 追加
                            '売却数': sell_qty, '売上額': sell_price, '手数料': fee,
                            '経費_送料': shipping_cost, '純利益': profit, '販路': channel,
                            '備考': note, '登録日時': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }])
                        df_sales = pd.concat([df_sales, new_sale], ignore_index=True) if not df_sales.empty else new_sale
                        save_sales_data(df_sales)
                        
                        st.success(f"🎉 売却完了！純利益は ¥{profit:,} です。（手数料: ¥{fee:,}）")
                        time.sleep(2)
                        st.rerun()

        with tab_edit:
            st.subheader("✏️ 全在庫データの編集・削除")
            df_edit = df.copy()
            df_edit['削除'] = False
            
            edited_df = st.data_editor(
                df_edit[['削除', '商品名', '収録パック', '種類', '状態_PSA', '在庫数', '原価', 'ステータス', '相場更新', 'ID', '参考相場']],
                hide_index=True,
                column_config={
                    "削除": st.column_config.CheckboxColumn("削除", default=False, width="small"), 
                    "ID": None,
                    "相場更新": st.column_config.CheckboxColumn("相場自動更新", default=True, help="ONにするとメンテナンス機能で最新相場を取得します"),
                    "参考相場": st.column_config.NumberColumn("参考相場", min_value=0, step=100)
                },
                use_container_width=True
            )
            
            if st.button("💾 変更・削除を保存する", type="primary"):
                ids_to_keep = edited_df[~edited_df['削除']]['ID'].tolist()
                df_saved = df[df['ID'].isin(ids_to_keep)].copy()
                for idx, row in edited_df.iterrows():
                    if not row['削除']:
                        df_saved.loc[df_saved['ID'] == row['ID'], '商品名'] = row['商品名']
                        df_saved.loc[df_saved['ID'] == row['ID'], '収録パック'] = row['収録パック']
                        df_saved.loc[df_saved['ID'] == row['ID'], '状態_PSA'] = row['状態_PSA']
                        df_saved.loc[df_saved['ID'] == row['ID'], '在庫数'] = row['在庫数']
                        df_saved.loc[df_saved['ID'] == row['ID'], '原価'] = row['原価']
                        df_saved.loc[df_saved['ID'] == row['ID'], 'ステータス'] = row['ステータス']
                        df_saved.loc[df_saved['ID'] == row['ID'], '相場更新'] = row['相場更新']
                        df_saved.loc[df_saved['ID'] == row['ID'], '参考相場'] = row['参考相場']
                save_data(df_saved)
                st.success("✅ データベースを更新しました！"); time.sleep(1.5); st.rerun()
                
        with tab_maintenance:
            st.subheader("🛠️ データベースのメンテナンス機能")
            
            with st.container(border=True):
                st.markdown("#### 🔗 在庫おまとめ（商品統合）")
                st.caption("「ホワイトフレア」のように、スペースの違い等で別々の行になってしまった商品を1つに合体させます。過去の帳簿の名前もすべて自動で統一します。")
                
                df_to_merge = df_active.copy()
                df_to_merge['統合対象'] = False
                
                merge_search = st.text_input("🔍 統合したい商品を検索", placeholder="例: ホワイトフレア")
                if merge_search:
                    df_to_merge = df_to_merge[df_to_merge['商品名'].str.contains(merge_search, case=False, na=False)]
                
                merge_editor = st.data_editor(
                    df_to_merge[['統合対象', '収録パック', '商品名', '種類', '状態_PSA', '在庫数', '原価', 'ID']],
                    hide_index=True,
                    column_config={
                        "統合対象": st.column_config.CheckboxColumn("選択", default=False, width="small"),
                        "ID": None
                    },
                    use_container_width=True,
                    key="merge_editor"
                )
                
                selected_for_merge = merge_editor[merge_editor['統合対象'] == True]
                
                if not selected_for_merge.empty:
                    if len(selected_for_merge) < 2:
                        st.info("統合するには2つ以上の商品を選択してください。")
                    else:
                        types = selected_for_merge['種類'].unique()
                        conds = selected_for_merge['状態_PSA'].unique()
                        
                        if len(types) > 1 or len(conds) > 1:
                            st.error("⚠️ 種類（BOXとシングル等）や状態が異なる商品は統合できません。")
                        else:
                            total_qty = selected_for_merge['在庫数'].sum()
                            total_cost_value = (selected_for_merge['原価'] * selected_for_merge['在庫数']).sum()
                            new_avg_cost = int(total_cost_value / total_qty) if total_qty > 0 else 0
                            
                            master_id = selected_for_merge.iloc[0]['ID']
                            master_row = df_active[df_active['ID'] == master_id].iloc[0].copy()
                            
                            master_name = master_row['商品名']
                            master_pack = master_row['収録パック']
                            
                            st.success(f"✅ 統合準備完了：以下の内容で1つにまとめます")
                            st.write(f"・名称: **{master_name}** / パック: **{master_pack}**")
                            st.write(f"・在庫数合計: **{total_qty}** / 新しい平均原価: **¥{new_avg_cost:,}**")
                            
                            if st.button("🚨 この内容で商品を統合し、過去の帳簿も書き換える", type="primary", use_container_width=True):
                                with st.spinner("データベースを統合中..."):
                                    ids_to_remove = selected_for_merge['ID'].tolist()
                                    df = df[~df['ID'].isin(ids_to_remove)]
                                    
                                    new_row = master_row.copy()
                                    new_row['在庫数'] = total_qty
                                    new_row['原価'] = new_avg_cost
                                    
                                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                                    save_data(df)
                                    
                                    df_pur_m = load_purchase_data()
                                    target_names = selected_for_merge['商品名'].tolist()
                                    target_packs = selected_for_merge['収録パック'].tolist()
                                    
                                    for t_name, t_pack in zip(target_names, target_packs):
                                        mask = (df_pur_m['商品名'] == t_name) & (df_pur_m['収録パック'] == t_pack)
                                        df_pur_m.loc[mask, '商品名'] = master_name
                                        df_pur_m.loc[mask, '収録パック'] = master_pack
                                    save_purchase_data(df_pur_m)
                                    
                                    df_sales_m = load_sales_data()
                                    for t_name, t_pack in zip(target_names, target_packs):
                                        mask = (df_sales_m['商品名'] == t_name) & (df_sales_m['収録パック'] == t_pack)
                                        df_sales_m.loc[mask, '商品名'] = master_name
                                        df_sales_m.loc[mask, '収録パック'] = master_pack
                                        df_sales_m.loc[df_sales_m['元の在庫ID'].isin(ids_to_remove), '元の在庫ID'] = master_id
                                    save_sales_data(df_sales_m)
                                    
                                    df_final = recalculate_moving_average_costs()
                                    save_data(df_final)
                                    
                                    st.success("✨ 商品の統合と帳簿の書き換えが完了しました！「神の計算機」との同期もバッチリです。")
                                    time.sleep(2); st.rerun()

            cc1, cc2 = st.columns(2)
            with cc1:
                with st.container(border=True):
                    st.markdown("#### 🧮 原価の全自動再計算（神の計算機）")
                    st.caption("入力ミスで原価がおかしくなった場合に押してください。過去の『全仕入』と『全売却』の履歴を最初から読み直し、1ミリの狂いもない正しい【平均原価】に在庫DBを修復・上書きします。")
                    if st.button("🚨 全在庫の原価を再計算する", use_container_width=True):
                        with st.spinner("タイムマシンで過去の履歴を計算中..."):
                            recalculated_df = recalculate_moving_average_costs()
                            save_data(recalculated_df)
                            st.success("✅ 全在庫の原価を正しく修復しました！")
                            time.sleep(2); st.rerun()

            with cc2:
                with st.container(border=True):
                    st.markdown("#### 🌐 最新相場の一括取得・更新")
                    st.caption("カードラッシュの最新価格を取得し、在庫の参考相場を上書きします。（※『データ編集』タブで【相場自動更新】がOFFになっているマイナー品はスキップされます）")
                    if st.button("🔄 自動更新ONのカードの相場を最新にする", use_container_width=True):
                        df_inv_maint = load_data()
                        items_to_update = df_inv_maint[(df_inv_maint['相場更新'] == True) & (df_inv_maint['ステータス'] != '売却済み')]
                        unique_queries = items_to_update[['商品名', '収録パック']].drop_duplicates()
                        
                        if unique_queries.empty:
                            st.info("更新対象のカードがありませんでした。")
                        else:
                            progress_text = st.empty()
                            progress_bar = st.progress(0)
                            total_items = len(unique_queries)
                            
                            for i, (_, row) in enumerate(unique_queries.iterrows()):
                                orig_name = row['商品名']
                                orig_pack = row['収録パック']
                                
                                search_kw = generate_search_keyword(orig_name)
                                progress_text.text(f"🔍 検索中: {search_kw} ({i+1}/{total_items})")
                                
                                time.sleep(1.0)
                                results = search_card_rush(search_kw)
                                
                                best_match = get_best_match(orig_name, orig_pack, results)
                                
                                if best_match:
                                    mask = (df_inv_maint['商品名'] == orig_name) & (df_inv_maint['収録パック'] == orig_pack)
                                    df_inv_maint.loc[mask, '参考相場'] = best_match['price']
                                
                                progress_bar.progress((i + 1) / total_items)
                                
                            save_data(df_inv_maint)
                            progress_text.text("✨ 完了しました！")
                            st.success("✅ 最新の相場に一括更新しました！")
                            time.sleep(2); st.rerun()

# =========================================================
# 🛍️ 第3フェーズ：オリパ工場
# =========================================================
elif menu == "🛍️ オリパ工場":
    st.header("🛍️ オリパ工場 (セット化・錬金)")
    st.write("在庫のカードや素材を組み合わせて、経費を精密計算したオリパを作成します。")
    
    df = load_data()
    if df.empty:
        st.warning("現在、オリパに使える在庫がありません。")
    else:
        df_available = df[(df['ステータス'] == '在庫あり') | (df['ステータス'] == '鑑定済み')].copy()
        
        if df_available.empty:
            st.warning("現在、オリパに使える有効な在庫がありません。")
        else:
            col_list, col_calc = st.columns([1.5, 1])
            with col_list:
                st.subheader("① 封入するカード・素材の選択")
                c_search, c_filter = st.columns([1.5, 1])
                with c_search: search_oripa = st.text_input("🔍 商品名で検索", placeholder="例: ピカチュウ, VSTARユニバース")
                with c_filter: filter_types = st.multiselect("種類で絞り込み", options=df_available['種類'].unique().tolist())
                
                if search_oripa: df_available = df_available[df_available['商品名'].str.contains(search_oripa, case=False, na=False)]
                if filter_types: df_available = df_available[df_available['種類'].isin(filter_types)]
                
                df_available['オリパに使う'] = False
                df_available['使用数'] = 0
                
                if not df_available.empty:
                    oripa_editor = st.data_editor(
                        df_available[['オリパに使う', '収録パック', '商品名', '種類', '原価', '在庫数', '使用数', 'ID']],
                        hide_index=True,
                        column_config={
                            "オリパに使う": st.column_config.CheckboxColumn("選択", default=False, width="small"),
                            "収録パック": st.column_config.TextColumn("パック", disabled=True, width="small"),
                            "商品名": st.column_config.TextColumn("商品名", disabled=True),
                            "種類": st.column_config.TextColumn("種類", disabled=True, width="small"),
                            "原価": st.column_config.NumberColumn("原価", disabled=True, format="¥%d", width="small"),
                            "在庫数": st.column_config.NumberColumn("現在庫", disabled=True, width="small"),
                            "使用数": st.column_config.NumberColumn("使う数", min_value=0, step=1, width="small"),
                            "ID": None
                        },
                        use_container_width=True
                    )
                    selected_items = oripa_editor[(oripa_editor['オリパに使う'] == True) & (oripa_editor['使用数'] > 0)]
                else:
                    st.info("該当する在庫がありません。")
                    selected_items = pd.DataFrame()
            
            with col_calc:
                st.subheader("② オリパの設定と利益計算")
                with st.container(border=True):
                    oripa_name = st.text_input("オリパの名称", placeholder="例: 春の激アツ！1000円オリパ")
                    c_qty, c_price = st.columns(2)
                    with c_qty: total_units = st.number_input("作成口数 (全口数)", min_value=1, value=100, step=10)
                    with c_price: unit_price = st.number_input("1口の販売価格 (円)", min_value=0, value=1000, step=100)
                    
                    c_ship, c_pack = st.columns(2)
                    with c_ship: shipping_cost = st.number_input("1口あたりの送料", min_value=0, value=185)
                    with c_pack: packing_cost = st.number_input("1口あたりの梱包費", min_value=0, value=50)
                
                if not selected_items.empty:
                    materials_total_cost = sum(selected_items['原価'] * selected_items['使用数'])
                    expenses_total_cost = (shipping_cost + packing_cost) * total_units
                    grand_total_cost = materials_total_cost + expenses_total_cost
                    cost_per_unit = int(grand_total_cost / total_units) if total_units > 0 else 0
                    
                    expected_sales = unit_price * total_units
                    expected_profit = expected_sales - grand_total_cost
                    
                    st.write(f"🃏 カード原価合計: **¥{materials_total_cost:,}**")
                    st.write(f"📦 経費合計(送料+梱包): **¥{expenses_total_cost:,}**")
                    st.write(f"💰 総原価: **¥{grand_total_cost:,}** (1口あたり: ¥{cost_per_unit:,})")
                    st.divider()
                    st.metric("見込み売上総額", f"¥{expected_sales:,}")
                    st.metric("見込み純利益 (完売時)", f"¥{expected_profit:,}", delta=f"利益率: {int((expected_profit/expected_sales)*100)}%" if expected_sales>0 else None)
                    
                    is_valid = True
                    for idx, row in selected_items.iterrows():
                        if row['使用数'] > row['在庫数']:
                            st.error(f"⚠️ [{row['収録パック']}]{row['商品名']} の使用数がオーバーしています！")
                            is_valid = False
                    
                    if is_valid and oripa_name:
                        if st.button("🔨 この内容でオリパを作成 (在庫を消費)", type="primary", use_container_width=True):
                            for idx, row in selected_items.iterrows():
                                item_id = row['ID']
                                new_qty = df.loc[df['ID'] == item_id, '在庫数'].values[0] - row['使用数']
                                if new_qty <= 0:
                                    df.loc[df['ID'] == item_id, '在庫数'] = 0
                                    df.loc[df['ID'] == item_id, 'ステータス'] = 'オリパ消費'
                                else:
                                    df.loc[df['ID'] == item_id, '在庫数'] = new_qty
                            
                            new_oripa_id = "O" + str(uuid.uuid4())[:7]
                            new_oripa = pd.DataFrame([{
                                'ID': new_oripa_id, '商品名': f"【オリパ】{oripa_name}", '収録パック': '', '種類': 'オリジナルパック',
                                '状態_PSA': '-', '仕入日': datetime.now().strftime('%Y-%m-%d'),
                                '原価': cost_per_unit, '参考相場': unit_price,
                                '在庫数': total_units, '仕入元': '自家製',
                                'ステータス': '在庫あり', 'PSA番号': '', '相場更新': False
                            }])
                            df = pd.concat([df, new_oripa], ignore_index=True)
                            save_data(df)
                            
                            # ✨ v4.4 修正点: オリパを作ったら「仕入帳」にも透明な領収書を発行して、神の計算機のバグを防ぐ
                            batch_id = "B" + str(uuid.uuid4())[:7]
                            record_purchase_items(batch_id, datetime.now().strftime('%Y-%m-%d'), "自家製オリパ作成", "自家製", "オリパ工場", [{
                                'name': f"【オリパ】{oripa_name}", 'pack': '', 'type': 'オリジナルパック', 'cond': '-',
                                'qty': total_units, 'unit_cost': cost_per_unit, 'subtotal': grand_total_cost
                            }])
                            
                            st.success(f"🎉 オリパが完成しました！"); time.sleep(2); st.rerun()

# =========================================================
# 📖 第4フェーズ：帳簿・分析 ＋ 📤 エクスポート機能
# =========================================================
elif menu == "📖 帳簿・分析":
    st.header("📖 帳簿・分析 (ダッシュボード)")
    
    df_inv = load_data()
    df_pur = load_purchase_data()
    df_sales = load_sales_data()
    
    tab_dash, tab_sales, tab_sales_undo, tab_pur, tab_pur_undo, tab_export = st.tabs([
        "📈 ダッシュボード", "📒 売上帳", "↩️ 売却取消", "📒 仕入帳", "↩️ 仕入取消", "📤 出力"
    ])
    
    with tab_dash:
        st.subheader("💰 現在の資産状況")
        if not df_inv.empty:
            df_active = df_inv[df_inv['ステータス'] != '売却済み']
            total_inv_cost = (df_active['原価'] * df_active['在庫数']).sum()
            total_market = (df_active['参考相場'] * df_active['在庫数']).sum()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("現在の在庫総額 (原価)", f"¥{total_inv_cost:,}")
            c2.metric("見込み売上総額 (相場)", f"¥{total_market:,}")
            c3.metric("含み益", f"¥{total_market - total_inv_cost:,}")
        else:
            st.info("在庫データがありません。")
            
        st.divider()
        st.subheader("✨ 確定済みの利益")
        if not df_sales.empty:
            total_sales_amt = df_sales['売上額'].sum()
            total_profit = df_sales['純利益'].sum()
            
            sc1, sc2 = st.columns(2)
            sc1.metric("累計売上高", f"¥{total_sales_amt:,}")
            sc2.metric("累計純利益", f"¥{total_profit:,}")
        else:
            st.info("まだ売上記録がありません。")

    with tab_sales:
        st.subheader("📒 売上履歴")
        if not df_sales.empty:
            st.dataframe(df_sales[['売却日', '収録パック', '商品名', '状態_PSA', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路']], hide_index=True, use_container_width=True)
        else:
            st.caption("データがありません。")

    with tab_sales_undo:
        st.subheader("↩️ 売却の取り消し (在庫戻し)")
        st.warning("間違えて売却登録した場合、ここから取り消しを行うと在庫数が元に戻り、売上帳から削除されます。")
        if not df_sales.empty:
            undo_opts = {f"{row['売却日']} | [{row['収録パック']}]{row['商品名']} ({row['状態_PSA']} | 売却数:{row['売却数']}) | 利益:¥{row['純利益']} [ID:{row['ID']}]": row['ID'] for idx, row in df_sales.iterrows()}
            target_undo = st.selectbox("取り消す取引を選択", options=list(undo_opts.keys()), index=None)
            
            if target_undo and st.button("🚨 この取引を取り消す", type="primary"):
                sale_id = undo_opts[target_undo]
                sale_row = df_sales[df_sales['ID'] == sale_id].iloc[0]
                restored_qty = int(sale_row['売却数'])
                target_cond = str(sale_row.get('状態_PSA', 'A (美品)')).strip()
                
                original_item_id = sale_row.get('元の在庫ID', '')
                
                if original_item_id:
                    match_inv = df_inv[df_inv['ID'] == original_item_id]
                else:
                    mask_name = df_inv['商品名'].astype(str).str.strip() == str(sale_row['商品名']).strip()
                    mask_cond = df_inv['状態_PSA'].astype(str).str.strip() == target_cond
                    match_inv = df_inv[mask_name & mask_cond]
                
                if not match_inv.empty:
                    target_inv_id = match_inv.iloc[0]['ID']
                    current_qty = int(match_inv.iloc[0]['在庫数'])
                    df_inv.loc[df_inv['ID'] == target_inv_id, '在庫数'] = current_qty + restored_qty
                    df_inv.loc[df_inv['ID'] == target_inv_id, 'ステータス'] = '在庫あり'
                    save_data(df_inv)
                else:
                    st.warning("元の在庫データが見つかりませんでしたが、売上記録の削除のみ行います。")

                df_sales_new = df_sales[df_sales['ID'] != sale_id]
                save_sales_data(df_sales_new)
                
                st.success("売却を取り消しました。在庫が元に戻りました。")
                time.sleep(2)
                st.rerun()
        else:
            st.caption("取り消せる売上記録がありません。")

    with tab_pur:
        st.subheader("📒 仕入履歴")
        if not df_pur.empty:
            st.dataframe(df_pur[['仕入日', '仕入先', '仕入名目', '収録パック', '商品名', '状態_PSA', '数量', '単価', '小計']], hide_index=True, use_container_width=True)
        else:
            st.caption("データがありません。")

    with tab_pur_undo:
        st.subheader("↩️ 仕入の取り消し (タイムマシン)")
        st.warning("間違えて仕入登録した場合、ここから取り消しを行うと在庫から個数が引かれ、仕入帳から履歴が消去されます。さらに原価も仕入前の状態に自動修復されます。")
        if not df_pur.empty:
            pur_undo_opts = {f"{row['仕入日']} | {row['仕入先']} | [{row['収録パック']}]{row['商品名']} ({row['状態_PSA']} | 数量:{row['数量']}) [ID:{row['ID']}]": row['ID'] for idx, row in df_pur.iterrows()}
            target_pur_undo = st.selectbox("取り消す仕入を選択してください", options=list(pur_undo_opts.keys()), index=None)
            
            if target_pur_undo and st.button("🚨 この仕入を取り消す", type="primary"):
                pur_id = pur_undo_opts[target_pur_undo]
                pur_row = df_pur[df_pur['ID'] == pur_id].iloc[0]
                cancel_qty = int(pur_row['数量'])
                target_name = str(pur_row['商品名']).strip()
                target_pack = str(pur_row.get('収録パック', '')).strip()
                target_cond = str(pur_row.get('状態_PSA', 'A (美品)')).strip()
                
                # ✨ v4.4 修正点: 指名手配の条件に「状態」を完全一致させるルールを追加
                mask_name = df_inv['商品名'].astype(str).str.strip() == target_name
                mask_pack = df_inv['収録パック'].astype(str).str.strip() == target_pack
                mask_cond = df_inv['状態_PSA'].astype(str).str.strip() == target_cond
                match_inv = df_inv[mask_name & mask_pack & mask_cond]
                
                if not match_inv.empty:
                    target_inv_id = match_inv.iloc[0]['ID']
                    current_qty = int(match_inv.iloc[0]['在庫数'])
                    new_qty = current_qty - cancel_qty
                    
                    if new_qty <= 0:
                        df_inv = df_inv[df_inv['ID'] != target_inv_id]
                    else:
                        df_inv.loc[df_inv['ID'] == target_inv_id, '在庫数'] = new_qty
                        
                    save_data(df_inv)
                else:
                    st.warning("対象の在庫データが見つかりませんでしたが、仕入記録の削除を続行します。")
                
                df_pur_new = df_pur[df_pur['ID'] != pur_id]
                save_purchase_data(df_pur_new)
                
                df_recalc = recalculate_moving_average_costs()
                save_data(df_recalc)

                st.success("仕入を取り消し、在庫の修正と原価の自動修復を完了しました！")
                time.sleep(2)
                st.rerun()
        else:
            st.caption("取り消せる仕入記録がありません。")

    with tab_export:
        st.subheader("📤 データのエクスポート (CSVダウンロード)")
        st.info("Excelで文字化けせずに直接開ける形式でダウンロードされます。")

        st.markdown("##### 📅 期間指定ダウンロード (確定申告・月次集計用)")
        c_start, c_end = st.columns(2)
        today = datetime.now().date()
        first_day = today.replace(day=1)
        
        with c_start:
            start_date = st.date_input("開始日", value=first_day)
        with c_end:
            end_date = st.date_input("終了日", value=today)

        if start_date > end_date:
            st.error("エラー: 開始日は終了日より前の日付を指定してください。")
        else:
            c_dl1, c_dl2 = st.columns(2)
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')

            with c_dl1:
                if not df_sales.empty:
                    filtered_sales = df_sales[(df_sales['売却日'] >= start_str) & (df_sales['売却日'] <= end_str)]
                    if not filtered_sales.empty:
                        csv_sales = filtered_sales.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(label="📥 指定期間の【売上帳】をダウンロード", data=csv_sales, file_name=f"売上帳_{start_str}_to_{end_str}.csv", mime='text/csv', key='dl_sales', use_container_width=True)
                    else:
                        st.button("📥 指定期間の【売上帳】をダウンロード", disabled=True, help="この期間のデータはありません", key='dl_sales_dis', use_container_width=True)
                else:
                    st.caption("売上データがありません")

            with c_dl2:
                if not df_pur.empty:
                    filtered_pur = df_pur[(df_pur['仕入日'] >= start_str) & (df_pur['仕入日'] <= end_str)]
                    if not filtered_pur.empty:
                        csv_pur = filtered_pur.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(label="📥 指定期間の【仕入帳】をダウンロード", data=csv_pur, file_name=f"仕入帳_{start_str}_to_{end_str}.csv", mime='text/csv', key='dl_pur', use_container_width=True)
                    else:
                        st.button("📥 指定期間の【仕入帳】をダウンロード", disabled=True, help="この期間のデータはありません", key='dl_pur_dis', use_container_width=True)
                else:
                    st.caption("仕入データがありません")

        st.divider()
        st.markdown("##### 📦 現在の在庫一覧ダウンロード (棚卸し・資産確認用)")
        if not df_inv.empty:
            inventory_active = df_inv[df_inv['ステータス'] != '売却済み']
            if not inventory_active.empty:
                csv_inv = inventory_active.to_csv(index=False).encode('utf-8-sig')
                st.download_button(label="📦 現在の【在庫一覧】をまるごとダウンロード", data=csv_inv, file_name=f"在庫棚卸表_{today.strftime('%Y%m%d')}.csv", mime='text/csv', key='dl_inv', type="primary")
            else:
                st.caption("有効な在庫がありません。")
        else:
            st.caption("在庫データがありません。")
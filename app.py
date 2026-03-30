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
# ⚙️ 設定・定数 (v3.0)
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
        ws_inv = sh.add_worksheet(title=SHEET_INVENTORY, rows=1000, cols=15)
        ws_inv.append_row(['ID', '商品名', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号'])

    try:
        ws_pur = sh.worksheet(SHEET_PURCHASE)
    except:
        ws_pur = sh.add_worksheet(title=SHEET_PURCHASE, rows=1000, cols=10)
        ws_pur.append_row(['ID', '仕入日', '仕入名目', '支払総額', '仕入先', '備考', '登録日時'])

    try:
        ws_sales = sh.worksheet(SHEET_SALES)
    except:
        ws_sales = sh.add_worksheet(title=SHEET_SALES, rows=1000, cols=12)
        ws_sales.append_row(['ID', '売却日', '商品名', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時'])

    return ws_inv, ws_pur, ws_sales

@st.cache_data(ttl=60)
def load_data():
    ws_inv, _, _ = check_and_init_sheets()
    if ws_inv:
        try:
            df = get_as_dataframe(ws_inv, evaluate_formulas=True)
            df = df.dropna(subset=['ID'])
            df = df[df['ID'] != '']
            
            if 'PSA番号' not in df.columns:
                df['PSA番号'] = ""
            
            df['原価'] = pd.to_numeric(df['原価'], errors='coerce').fillna(0).astype(int)
            df['参考相場'] = pd.to_numeric(df['参考相場'], errors='coerce').fillna(0).astype(int)
            df['在庫数'] = pd.to_numeric(df['在庫数'], errors='coerce').fillna(0).astype(int)
            df['PSA番号'] = df['PSA番号'].astype(str).replace('nan', '')
            
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    ws_inv, _, _ = check_and_init_sheets()
    if ws_inv:
        save_cols = ['ID', '商品名', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス', 'PSA番号']
        
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns:
                df_to_save[col] = ""
        
        df_to_save = df_to_save[save_cols]
        ws_inv.clear()
        set_with_dataframe(ws_inv, df_to_save)
        load_data.clear()

def record_purchase_batch(batch_id, date, title, total_paid, source, note):
    _, ws_pur, _ = check_and_init_sheets()
    if ws_pur:
        row = [batch_id, date, title, total_paid, source, note, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        ws_pur.append_row(row)

# ---------------------------------------------------------
# 🌐 スクレイピング＆文字列クリーニング (NEW)
# ---------------------------------------------------------
def clean_product_name(text):
    """カードラッシュ特有の余分な装飾を削ぎ落として綺麗な名前にする"""
    if not isinstance(text, str): return str(text)
    
    # {-} 以降（Card Rush特有の管理タグ）を削除
    text = re.sub(r'\{-}.*$', '', text)
    # 先頭の〔状態A-〕などを削除
    text = re.sub(r'^〔[^〕]+〕', '', text)
    text = re.sub(r'^【[^】]+】', '', text)
    # 【未開封BOX】などを削除
    text = re.sub(r'【未開封BOX】', '', text)
    text = re.sub(r'\[[^\]]+\]', '', text)
    
    # BOXの場合、「拡張パック『〇〇』」の〇〇だけを抽出する
    match = re.search(r'『([^』]+)』', text)
    if match:
        text = match.group(1)
        
    return text.strip()

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
            
            # BOXかどうかの判定と名前のクリーニング
            is_box = "BOX" in raw_name.upper() or "ｂｏｘ" in raw_name.lower()
            clean_name = clean_product_name(raw_name)
            
            # 元々BOXだったなら、綺麗にした名前に「 BOX」を付け直す
            if is_box and "BOX" not in clean_name.upper():
                clean_name = f"{clean_name} BOX"
            
            price = 0
            price_tag = item.select_one('.figure, .price, .goods_price')
            if price_tag:
                nums = re.findall(r'\d+', price_tag.get_text(strip=True).replace(',', ''))
                if nums: price = int(nums[0])
            
            img_url = ""
            img_tag = item.select_one('img')
            if img_tag and 'src' in img_tag.attrs:
                img_url = img_tag['src']
                if img_url.startswith('/'):
                    img_url = "https://www.cardrush-pokemon.jp" + img_url

            if price > 0:
                results.append({"name": clean_name, "price": price, "image": img_url})
        
        unique_results = []
        seen = set()
        for r in results:
            if r['name'] not in seen:
                unique_results.append(r)
                seen.add(r['name'])
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
    return results[:20]

# ---------------------------------------------------------
# 🖥️ アプリ画面 (v3.0)
# ---------------------------------------------------------
st.set_page_config(page_title="ぽっけぇ〜道 システム", layout="wide")
st.title("🎴 ぽっけぇ〜道 管理システム v3.0")

if 'cart' not in st.session_state: st.session_state['cart'] = []
if 'has_searched' not in st.session_state: st.session_state['has_searched'] = False

menu = st.sidebar.radio(
    "【作業メニュー】", 
    ["📦 スピード仕入・解体", "📊 在庫・PSA管理", "🛍️ オリパ工場", "📖 帳簿・分析"]
)

if menu == "📦 スピード仕入・解体":
    st.header("📦 スピード仕入・福袋解体 (カート式)")
    st.write("単品の仕入れから、福袋やオリパの解体まで、中身をカートに入れて一括登録します。")
    
    col_left, col_right = st.columns([1.2, 1])
    
    with col_left:
        st.subheader("① 商品を探してカートに入れる")
        tab_search, tab_bulk, tab_supply = st.tabs(["🔍 カード/BOX検索", "🗃️ 素材(バルク)", "📦 サプライ品"])
        
        with tab_search:
            search_word = st.text_input("カード名・BOX名を入力", placeholder="例: クレイバースト BOX, ナンジャモ SAR")
            if st.button("検索", type="primary", use_container_width=True):
                if search_word:
                    with st.spinner("カードラッシュを検索中..."):
                        res = search_card_rush(search_word)
                        st.session_state['search_res'] = res
                        st.session_state['has_searched'] = True
                else:
                    st.warning("キーワードを入力してください。")
            
            if st.session_state.get('has_searched'):
                if st.session_state.get('search_res'):
                    st.write("---")
                    for item in st.session_state['search_res']:
                        c1, c2, c3 = st.columns([1, 3, 2])
                        with c1:
                            if item['image']: st.image(item['image'], width=50)
                            else: st.write("🖼️ 画像なし")
                        with c2:
                            st.write(f"**{item['name']}**")
                            st.caption(f"相場: ¥{item['price']:,}")
                        with c3:
                            with st.popover("カートに追加"):
                                add_qty = st.number_input("枚数/個数", min_value=1, value=1, key=f"qty_{item['name']}")
                                add_cond = st.selectbox("状態", ["A (美品)", "S (完美品)", "B (傷有)", "プレイ用", "未開封"], key=f"cond_{item['name']}")
                                if st.button("追加する", key=f"add_{item['name']}", use_container_width=True):
                                    st.session_state['cart'].append({
                                        "id": str(uuid.uuid4())[:8],
                                        "name": item['name'],
                                        "type": "未開封BOX" if "BOX" in item['name'].upper() else "シングルカード",
                                        "cond": add_cond,
                                        "qty": add_qty,
                                        "market_price": item['price']
                                    })
                                    st.success("追加しました！")
                                    time.sleep(0.5)
                                    st.rerun()
                    st.write("---")
                else:
                    st.error("見つかりませんでした。別のキーワードで試してください。")
        
        with tab_bulk:
            st.info("オリパ作成用のハズレ枠素材を一括追加します。")
            bulk_type = st.selectbox("素材の種類", ["【素材】SR", "【素材】AR", "【素材】RR", "【素材】CHR", "【素材】K", "【素材】汎用ノーマル"])
            bulk_price = st.number_input("1枚あたりの相場（価値）", min_value=0, value=30, step=10)
            bulk_qty = st.number_input("枚数", min_value=1, value=100, step=10)
            if st.button("素材をカートに追加", use_container_width=True):
                st.session_state['cart'].append({
                    "id": str(uuid.uuid4())[:8],
                    "name": bulk_type,
                    "type": "素材・バルク",
                    "cond": "プレイ用",
                    "qty": bulk_qty,
                    "market_price": bulk_price
                })
                st.success(f"{bulk_type} を {bulk_qty}枚 追加しました！")
                st.rerun()

        with tab_supply:
            st.info("スリーブや梱包材などの経費用品を追加します（在庫DBには入らず、帳簿のみに記録されます）。")
            sup_name = st.text_input("品名", placeholder="例: 100均スリーブ, ローダー100枚")
            sup_qty = st.number_input("個数", min_value=1, value=1)
            sup_price = st.number_input("1個あたりの金額", min_value=0, step=100)
            if st.button("サプライをカートに追加", use_container_width=True):
                if sup_name:
                    st.session_state['cart'].append({
                        "id": str(uuid.uuid4())[:8],
                        "name": f"【サプライ】{sup_name}",
                        "type": "サプライ",
                        "cond": "-",
                        "qty": sup_qty,
                        "market_price": sup_price
                    })
                    st.success("追加しました！")
                    st.rerun()
                else:
                    st.warning("品名を入力してください")

    with col_right:
        st.subheader("② カートの中身と原価計算")
        
        with st.container(border=True):
            st.markdown("##### 🧾 今回の支払い情報")
            total_paid = st.number_input("支払った総額 (送料・手数料込み)", min_value=0, value=0, step=1000)
            purchase_title = st.text_input("仕入名目 (任意)", placeholder="例: 秋葉原福袋, メルカリまとめ売り")
            purchase_source = st.selectbox("仕入先", ["店舗", "フリマ(メルカリ等)", "オンラインオリパ", "問屋", "その他"])
            
        st.markdown("##### 🛒 現在のカート")
        if not st.session_state['cart']:
            st.caption("カートは空です。左から商品を追加してください。")
        else:
            total_market_value = sum(item['qty'] * item['market_price'] for item in st.session_state['cart'])
            
            calculated_cart = []
            for item in st.session_state['cart']:
                item_total_market = item['qty'] * item['market_price']
                if total_market_value > 0:
                    ratio = item_total_market / total_market_value
                    allocated_cost_total = total_paid * ratio
                    unit_cost = int(allocated_cost_total / item['qty'])
                else:
                    unit_cost = 0
                
                calculated_cart.append({
                    "削除": False,
                    "ID": item['id'],
                    "商品名": item['name'],
                    "種類": item['type'],
                    "数量": item['qty'],
                    "自動計算原価": unit_cost,
                    "参考相場": item['market_price']
                })
            
            calc_df = pd.DataFrame(calculated_cart)
            
            st.write(f"💡 カート内の相場合計: **¥{total_market_value:,}**")
            st.caption("※以下の「自動計算原価」は、支払総額を相場比率で自動配分した結果です。")
            
            edited_cart = st.data_editor(
                calc_df,
                hide_index=True,
                column_config={
                    "削除": st.column_config.CheckboxColumn("削除", width="small", default=False),
                    "ID": None,
                    "自動計算原価": st.column_config.NumberColumn("原価/個", format="¥%d"),
                    "参考相場": st.column_config.NumberColumn("相場/個", format="¥%d"),
                },
                use_container_width=True
            )
            
            if edited_cart['削除'].any():
                if st.button("🗑️ チェックした商品をカートから外す"):
                    ids_to_keep = edited_cart[~edited_cart['削除']]['ID'].tolist()
                    st.session_state['cart'] = [item for item in st.session_state['cart'] if item['id'] in ids_to_keep]
                    st.rerun()

            st.divider()
            
            if st.button("✨ この内容で在庫DBと帳簿に一括登録 ✨", type="primary", use_container_width=True):
                df_inv = load_data()
                batch_id = "B" + str(uuid.uuid4())[:7]
                purchase_date = datetime.now().strftime('%Y-%m-%d')
                
                new_inventory_rows = []
                for idx, row in edited_cart.iterrows():
                    item_id = row['ID']
                    original_item = next(item for item in st.session_state['cart'] if item['id'] == item_id)
                    
                    if original_item['type'] != "サプライ":
                        new_inventory_rows.append({
                            'ID': item_id,
                            '商品名': row['商品名'],
                            '種類': row['種類'],
                            '状態_PSA': original_item['cond'],
                            '仕入日': purchase_date,
                            '原価': row['自動計算原価'],
                            '参考相場': row['参考相場'],
                            '在庫数': row['数量'],
                            '仕入元': purchase_source,
                            'ステータス': '在庫あり',
                            'PSA番号': ''
                        })
                
                if new_inventory_rows:
                    new_inv_df = pd.DataFrame(new_inventory_rows)
                    if not df_inv.empty:
                        df_combined = pd.concat([df_inv, new_inv_df], ignore_index=True)
                    else:
                        df_combined = new_inv_df
                    save_data(df_combined)
                
                record_title = purchase_title if purchase_title else f"一括仕入 ({len(st.session_state['cart'])}点)"
                record_purchase_batch(batch_id, purchase_date, record_title, total_paid, purchase_source, "カート一括登録")
                
                st.session_state['cart'] = []
                st.session_state['has_searched'] = False
                if 'search_res' in st.session_state: del st.session_state['search_res']
                
                st.success("🎉 全てのデータを在庫と帳簿に一括登録しました！")
                time.sleep(2)
                st.rerun()

# =========================================================
# 📊 第2フェーズ：在庫・PSA管理
# =========================================================
elif menu == "📊 在庫・PSA管理":
    st.header("📊 在庫・PSA管理")
    df = load_data()
    
    if df.empty:
        st.info("現在、データベースに在庫がありません。「スピード仕入」から商品を登録してください。")
    else:
        df_active = df[df['ステータス'] != '売却済み'].copy()
        
        tab_singles, tab_box_bulk, tab_psa = st.tabs(["🃏 シングル在庫", "📦 BOX・素材 (平均単価)", "💎 PSA管理"])
        
        with tab_singles:
            st.subheader("🃏 シングルカード在庫 (PSA以外)")
            df_single = df_active[(df_active['種類'] == 'シングルカード') & 
                                  (~df_active['ステータス'].isin(['PSA提出中', '鑑定済み']))]
            
            if not df_single.empty:
                st.dataframe(
                    df_single[['商品名', '状態_PSA', '原価', '在庫数', '仕入日', 'ステータス']], 
                    use_container_width=True, hide_index=True
                )
                
                st.divider()
                st.markdown("##### 🚀 PSA鑑定へ提出する")
                single_options = {f"{row['商品名']} (ID: {row['ID']})": row['ID'] for idx, row in df_single.iterrows()}
                target_to_psa = st.selectbox("提出するカードを選択してください", options=list(single_options.keys()), index=None)
                
                if target_to_psa and st.button("✈️ 選択したカードを「PSA提出中」にする", type="primary"):
                    target_id = single_options[target_to_psa]
                    df.loc[df['ID'] == target_id, 'ステータス'] = 'PSA提出中'
                    save_data(df)
                    st.success("PSA提出中に変更しました！")
                    time.sleep(1)
                    st.rerun()
            else:
                st.caption("現在、該当するシングルカードの在庫はありません。")

        with tab_box_bulk:
            st.subheader("📦 未開封BOX・素材 (自動平均化)")
            st.write("別々に仕入れた同じBOXや素材も、ここで自動的に合算され「平均原価」として表示されます。")
            
            df_bb = df_active[df_active['種類'].isin(['未開封BOX', '素材・バルク'])].copy()
            
            if not df_bb.empty:
                df_bb['総原価'] = df_bb['原価'] * df_bb['在庫数']
                grouped = df_bb.groupby(['種類', '商品名']).agg(
                    合計在庫数=('在庫数', 'sum'),
                    合計総原価=('総原価', 'sum')
                ).reset_index()
                
                grouped['平均単価(原価)'] = (grouped['合計総原価'] / grouped['合計在庫数']).fillna(0).astype(int)
                
                st.dataframe(
                    grouped[['種類', '商品名', '合計在庫数', '平均単価(原価)']], 
                    use_container_width=True, hide_index=True
                )
            else:
                st.caption("現在、未開封BOXや素材の在庫はありません。")

        with tab_psa:
            st.subheader("💎 PSA管理 (提出中・鑑定済み)")
            
            df_psa_pending = df_active[df_active['ステータス'] == 'PSA提出中']
            df_psa_done = df_active[df_active['ステータス'] == '鑑定済み']
            
            col_p1, col_p2 = st.columns(2)
            
            with col_p1:
                st.markdown("##### ⏳ PSA提出中")
                if not df_psa_pending.empty:
                    st.dataframe(df_psa_pending[['商品名', '原価', '仕入日']], hide_index=True, use_container_width=True)
                else:
                    st.caption("現在、提出中のカードはありません。")
                    
            with col_p2:
                st.markdown("##### ✨ 鑑定済み (ストック)")
                if not df_psa_done.empty:
                    st.dataframe(df_psa_done[['商品名', '状態_PSA', 'PSA番号', '原価']], hide_index=True, use_container_width=True)
                else:
                    st.caption("現在、鑑定済みのカードはありません。")

            st.divider()
            
            if not df_psa_pending.empty:
                st.markdown("##### 📥 鑑定結果の登録（原価への費用加算）")
                psa_options = {f"{row['商品名']} (ID: {row['ID']})": row['ID'] for idx, row in df_psa_pending.iterrows()}
                
                with st.form("psa_return_form"):
                    target_return = st.selectbox("戻ってきたカードを選択", options=list(psa_options.keys()))
                    
                    c1, c2, c3 = st.columns(3)
                    with c1: grade = st.selectbox("鑑定結果 (グレード)", ["10", "9", "8", "7以下"])
                    with c2: cert = st.text_input("PSA証明番号 (Cert #)", placeholder="例: 12345678")
                    with c3: fee = st.number_input("かかった鑑定費用 (円)", min_value=0, value=3300, step=100)
                    
                    if st.form_submit_button("鑑定結果を登録して原価を更新", type="primary"):
                        target_id = psa_options[target_return]
                        df.loc[df['ID'] == target_id, '原価'] += fee
                        df.loc[df['ID'] == target_id, 'ステータス'] = '鑑定済み'
                        df.loc[df['ID'] == target_id, '状態_PSA'] = f"PSA {grade}"
                        df.loc[df['ID'] == target_id, 'PSA番号'] = cert
                        
                        save_data(df)
                        st.success(f"鑑定結果を登録しました！原価に {fee}円 を加算しました。")
                        time.sleep(1.5)
                        st.rerun()

elif menu == "🛍️ オリパ工場":
    st.header("🛍️ オリパ工場")
    st.info("💡 ここに在庫からカードを引き落とし、送料や梱包費を個別に設定してオリパをセット化する機能を実装します。（次回アップデート予定）")

elif menu == "📖 帳簿・分析":
    st.header("📖 帳簿・分析")
    st.info("💡 ここにBASEやメルカリの手数料を自動計算した「真の純利益」や、資産状況のダッシュボードを実装します。（次回アップデート予定）")
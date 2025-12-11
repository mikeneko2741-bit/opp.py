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
# 設定・定数
# ---------------------------------------------------------
JSON_KEY_FILE = 'secrets.json'
SPREADSHEET_NAME = 'ポケカ在庫管理DB'

EXPANSION_LIST = {
    "--- MEGAシリーズ (2025~) ---": "",
    "MEGAドリームex (M2a)": "M2a",
    "インフェルノX (M2)": "M2",
    "メガシンフォニア (M1S)": "M1S",
    "メガブレイブ (M1L)": "M1L",
    "--- スカーレット・バイオレット (SV) ---": "",
    "ブラックボルト (SV11B)": "SV11B",
    "ホワイトフレア (SV11W)": "SV11W",
    "ロケット団の栄光 (SV10a)": "SV10a",
    "熱風のアリーナ (SV10)": "SV10",
    "バトルパートナーズ (SV9)": "SV9",
    "テラスタルフェス (SV8a)": "SV8a",
    "超電ブレイカー (SV8)": "SV8",
    "楽園ドラゴーナ (SV7a)": "SV7a",
    "ステラミラクル (SV7)": "SV7",
    "ナイトワンダラー (SV6a)": "SV6a",
    "変幻の仮面 (SV6)": "SV6",
    "クリムゾンヘイズ (SV5a)": "SV5a",
    "サイバージャッジ (SV5M)": "SV5M",
    "ワイルドフォース (SV5K)": "SV5K",
    "シャイニートレジャーex (SV4a)": "SV4a",
    "古代の咆哮 (SV4K)": "SV4K",
    "未来の一閃 (SV4M)": "SV4M",
    "レイジングサーフ (SV3a)": "SV3a",
    "黒炎の支配者 (SV3)": "SV3",
    "ポケモンカード151 (SV2a)": "SV2a",
    "クレイバースト (SV2D)": "SV2D",
    "スノーハザード (SV2P)": "SV2P",
    "トリプレットビート (SV1a)": "SV1a",
    "バイオレットex (SV1V)": "SV1V",
    "スカーレットex (SV1S)": "SV1S",
    "--- ソード・シールド (S) ---": "",
    "VSTARユニバース (S12a)": "S12a",
    "パラダイムトリガー (S12)": "S12",
    "白熱のアルカナ (S11a)": "S11a",
    "ロストアビス (S11)": "S11",
    "ポケモンGO (S10b)": "S10b",
    "ダークファンタズマ (S10a)": "S10a",
    "タイムゲイザー (S10D)": "S10D",
    "スペースジャグラー (S10P)": "S10P",
    "バトルリージョン (S9a)": "S9a",
    "スターバース (S9)": "S9",
    "VMAXクライマックス (S8b)": "S8b",
    "25th ANNIVERSARY COL (S8a)": "S8a",
    "フュージョンアーツ (S8)": "S8",
    "蒼空ストリーム (S7R)": "S7R",
    "摩天パーフェクト (S7D)": "S7D",
    "イーブイヒーローズ (S6a)": "S6a",
    "漆黒のガイスト (S6K)": "S6K",
    "白銀のランス (S6H)": "S6H",
    "双璧のファイター (S5a)": "S5a",
    "一撃マスター (S5I)": "S5I",
    "連撃マスター (S5R)": "S5R",
    "シャイニースターV (S4a)": "S4a",
    "仰天のボルテッカー (S4)": "S4",
    "伝説の鼓動 (S3a)": "S3a",
    "ムゲンゾーン (S3)": "S3",
    "爆炎ウォーカー (S2a)": "S2a",
    "反逆クラッシュ (S2)": "S2",
    "VMAXライジング (S1a)": "S1a",
    "ソード (S1W)": "S1W",
    "シールド (S1H)": "S1H",
    "--- サン・ムーン (SM) ---": "",
    "タッグオールスターズ (SM12a)": "SM12a",
    "オルタージェネシス (SM12)": "SM12",
    "ドリームリーグ (SM11b)": "SM11b",
    "ミラクルツイン (SM11)": "SM11",
    "スカイレジェンド (SM10b)": "SM10b",
    "ジージーエンド (SM10a)": "SM10a",
    "ダブルブレイズ (SM10)": "SM10",
    "フルメタルウォール (SM9b)": "SM9b",
    "ナイトユニゾン (SM9a)": "SM9a",
    "タッグボルト (SM9)": "SM9",
    "ウルトラシャイニー (SM8b)": "SM8b",
    "ダークオーダー (SM8a)": "SM8a",
    "超爆インパクト (SM8)": "SM8",
    "フェアリーライズ (SM7b)": "SM7b",
    "迅雷スパーク (SM7a)": "SM7a",
    "裂空のカリスマ (SM7)": "SM7",
    "チャンピオンロード (SM6b)": "SM6b",
    "ドラゴンストーム (SM6a)": "SM6a",
    "禁断の光 (SM6)": "SM6",
    "ウルトラフォース (SM5+)": "SM5+",
    "ウルトラサン (SM5S)": "SM5S",
    "ウルトラサン (SM5S)": "SM5S",
    "ウルトラムーン (SM5M)": "SM5M",
    "GXバトルブースト (SM4+)": "SM4+",
    "覚醒の勇者 (SM4S)": "SM4S",
    "超次元の暴獣 (SM4A)": "SM4A",
    "ひかる伝説 (SM3+)": "SM3+",
    "闘う虹を見たか (SM3H)": "SM3H",
    "光を喰らう闇 (SM3N)": "SM3N",
    "新たなる試練の向こう (SM2+)": "SM2+",
    "キミを待つ島々 (SM2K)": "SM2K",
    "アローラの月光 (SM2L)": "SM2L",
    "サン＆ムーン (SM1+)": "SM1+",
    "コレクションサン (SM1S)": "SM1S",
    "コレクションムーン (SM1M)": "SM1M",
    "--- XYシリーズ ---": "",
    "THE BEST OF XY (XY)": "XY",
    "20th Anniversary (CP6)": "CP6",
    "幻・伝説ドリームキラ (CP5)": "CP5",
    "EX×M×BREAK (CP4)": "CP4",
    "ポケキュンコレクション (CP3)": "CP3",
    "爆熱の闘士 (XY11)": "XY11",
    "冷酷の反逆者 (XY11)": "XY11",
    "めざめる超王 (XY10)": "XY10",
    "破天の怒り (XY9)": "XY9",
    "赤い閃光 (XY8)": "XY8",
    "青い衝撃 (XY8)": "XY8",
    "バンデットリング (XY7)": "XY7",
    "エメラルドブレイク (XY6)": "XY6",
    "ガイアボルケーノ (XY5)": "XY5",
    "タイダルストーム (XY5)": "XY5",
    "ファントムゲート (XY4)": "XY4",
    "ライジングフィスト (XY3)": "XY3",
    "ワイルドブレイズ (XY2)": "XY2",
    "コレクションX (XY1)": "XY1",
    "コレクションY (XY1)": "XY1",
    "--- BWシリーズ ---": "",
    "EXバトルブースト (EBB)": "EBB",
    "メガロキャノン (BW9)": "BW9",
    "ライデンナックル (BW8)": "BW8",
    "ラセンフォース (BW8)": "BW8",
    "プラズマゲイル (BW7)": "BW7",
    "フリーズボルト (BW6)": "BW6",
    "コールドフレア (BW6)": "BW6",
    "リューノブレード (BW5)": "BW5",
    "リューズブラスト (BW5)": "BW5",
    "ダークラッシュ (BW4)": "BW4",
    "サイコドライブ (BW3)": "BW3",
    "ヘイルブリザード (BW3)": "BW3",
    "レッドコレクション (BW2)": "BW2",
    "ホワイトコレクション (BW1)": "BW1",
    "ブラックコレクション (BW1)": "BW1",
    "--- その他・旧裏 ---": "",
    "プロモカード (PROMO)": "PROMO",
    "旧裏面": "OLD",
    "その他": "OTHER"
}

# ---------------------------------------------------------
# データ読み書き機能
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
            st.error("認証キーが見つかりません。")
            return None

        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        client = gspread.authorize(creds)
        return client

    except Exception as e:
        st.error(f"認証エラーが発生しました。\n詳細: {e}")
        return None

def get_sheet():
    client = get_gspread_client()
    if client:
        try:
            sheet = client.open(SPREADSHEET_NAME).sheet1
            return sheet
        except gspread.exceptions.SpreadsheetNotFound:
            st.error(f"スプレッドシート「{SPREADSHEET_NAME}」が見つかりません。")
            return None
    return None

def load_data():
    sheet = get_sheet()
    if sheet:
        try:
            df = get_as_dataframe(sheet, evaluate_formulas=True)
        except Exception:
            df = pd.DataFrame()

        # 自動修復機能
        if df.empty or 'ID' not in df.columns:
            required_cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', 
                             '仕入れ値', '想定売値', '参考販売', '参考買取', '保管場所', 'ステータス', 'PSA番号', '在庫数']
            df_fresh = pd.DataFrame(columns=required_cols)
            # シートが空の時だけ書き込む（安全策）
            if df.empty:
                set_with_dataframe(sheet, df_fresh)
            return df_fresh

        df = df.dropna(subset=['ID'])
        df = df[df['ID'] != '']
        
        required_cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', 
                         '仕入れ値', '想定売値', '参考販売', '参考買取', '保管場所', 'ステータス', 'PSA番号', '在庫数']
        
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""

        str_cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', '保管場所', 'ステータス', 'PSA番号']
        for col in str_cols:
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
            if col == 'PSA番号':
                df[col] = df[col].apply(lambda x: x.replace(".0", "") if x.endswith(".0") else x)

        num_cols = ['仕入れ値', '想定売値', '参考販売', '参考買取']
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df['在庫数'] = pd.to_numeric(df['在庫数'], errors='coerce').fillna(1).astype(int)

        return df
    else:
        return pd.DataFrame(columns=['ID'])

def save_data(df):
    sheet = get_sheet()
    if sheet:
        save_cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', 
                     '仕入れ値', '想定売値', '参考販売', '参考買取', '保管場所', 'ステータス', 'PSA番号', '在庫数']
        
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns:
                df_to_save[col] = ""
        
        for col in ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', '保管場所', 'ステータス', 'PSA番号']:
            df_to_save[col] = df_to_save[col].astype(str).replace('nan', '')
        
        df_to_save['在庫数'] = df_to_save['在庫数'].fillna(1).astype(int)

        df_to_save = df_to_save[save_cols]
        sheet.clear()
        set_with_dataframe(sheet, df_to_save)

# ---------------------------------------------------------
# スクレイピング & クリーニング
# ---------------------------------------------------------
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
            name = name_tag.get_text(strip=True)
            price = 0
            price_tag = item.select_one('.figure, .price, .goods_price')
            if price_tag:
                price_text = price_tag.get_text(strip=True).replace(',', '')
                nums = re.findall(r'\d+', price_text)
                if nums: price = int(nums[0])
            if price > 0:
                results.append({"name": name, "price": price})
        
        unique_results = []
        seen_names = set()
        for r in results:
            if r['name'] not in seen_names:
                unique_results.append(r)
                seen_names.add(r['name'])
        return unique_results
    except Exception:
        return []

def search_card_rush(keyword):
    base_url = "https://www.cardrush-pokemon.jp"
    encoded_keyword = quote(keyword.encode('utf-8'))
    url_a = f"{base_url}/product-list?keyword={encoded_keyword}&num=100"
    results_a = fetch_from_url(url_a)
    if len(results_a) > 1: return results_a[:50]
    url_b = f"{base_url}/shop/shopbrand.html?search={encoded_keyword}"
    results_b = fetch_from_url(url_b)
    if len(results_b) > len(results_a): return results_b[:50]
    else: return results_a[:50]

def clean_product_name(text):
    if not isinstance(text, str): return str(text)
    text = re.sub(r'[【\[\(\{（〔].*?[】\]\)\}）〕]', '', text)
    text = re.sub(r'^[A-Za-z0-9]+[-]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ---------------------------------------------------------
# アプリ画面
# ---------------------------------------------------------
st.set_page_config(page_title="ポケカ在庫管理", layout="wide")
st.title("🎴 ポケカ在庫・収支管理システム (Cloud版)")

df = load_data()
menu = st.sidebar.radio("メニュー", ["📦 在庫登録", "📊 在庫一覧・編集", "💰 収支分析"])

# ==========================================
# 1. 在庫登録画面
# ==========================================
if menu == "📦 在庫登録":
    st.header("新規在庫の登録")
    with st.expander("➕ 新規在庫を登録する (ここをタップして開閉)", expanded=True):
        reg_mode = st.radio("登録モード", ["🃏 シングルカード", "📦 未開封BOX"], horizontal=True)
        st.subheader("① 商品検索 (販売価格)")
        search_tab1, search_tab2 = st.tabs(["🔢 型番/パックで検索", "🔤 キーワード検索"])
        search_keyword = ""
        
        with search_tab1:
            col_search1, col_search2 = st.columns([2, 1])
            with col_search1:
                selected_exp_name = st.selectbox("エキスパンション", list(EXPANSION_LIST.keys()), index=1)
                expansion_code = EXPANSION_LIST[selected_exp_name]
            with col_search2:
                if reg_mode == "🃏 シングルカード": card_number = st.text_input("カード番号", placeholder="例: 100")
                else: st.info("BOX名で検索"); card_number = ""
            if st.button("🔍 型番で検索", key="btn_search_code", use_container_width=True):
                if reg_mode == "🃏 シングルカード":
                    if expansion_code and card_number: search_keyword = f"{expansion_code} {card_number}"
                    else: st.warning("パックと番号を入力してください")
                else:
                    if selected_exp_name and selected_exp_name != "選択してください":
                        exp_name_only = selected_exp_name.split("(")[0].strip()
                        search_keyword = f"{exp_name_only} BOX"
                    else: st.warning("エキスパンションを選択してください")

        with search_tab2:
            free_word = st.text_input("カード名 / 商品名", placeholder="例: ピカチュウ, ナンジャモ, ミモザ")
            if st.button("🔍 名前で検索", key="btn_search_name", use_container_width=True):
                if free_word: search_keyword = free_word
                else: st.warning("キーワードを入力してください")

        if 'search_candidates' not in st.session_state: st.session_state['search_candidates'] = []
        if 'selected_item' not in st.session_state: st.session_state['selected_item'] = None

        if search_keyword:
            with st.spinner('カードラッシュから情報を取得中... (複数ルート検索)'):
                results = search_card_rush(search_keyword)
                st.session_state['search_candidates'] = results
                st.session_state['selected_item'] = None
                if not results: st.error("見つかりませんでした。")

        if st.session_state['search_candidates'] and not st.session_state['selected_item']:
            st.info(f"💡 {len(st.session_state['search_candidates'])} 件見つかりました。登録する商品を選択してください。")
            st.write("---")
            for i, item in enumerate(st.session_state['search_candidates']):
                c1, c2, c3 = st.columns([3, 1, 1])
                with c1: st.write(f"**{item['name']}**")
                with c2: st.write(f"¥{item['price']:,}")
                with c3:
                    if st.button("選択", key=f"sel_{i}", use_container_width=True):
                        st.session_state['selected_item'] = item
                        st.session_state['search_candidates'] = []
                        st.rerun()
            st.write("---")

        initial_name = ""
        initial_sales = 0
        if st.session_state['selected_item']:
            res = st.session_state['selected_item']
            initial_name = res['name']
            initial_sales = res['price']
            st.success(f"選択中: {initial_name}")
            st.info(f"🛒 現在の販売相場: ¥{initial_sales:,}")
            if st.button("やり直す"):
                st.session_state['selected_item'] = None
                st.rerun()

        st.divider()
        default_category = "シングルカード" if reg_mode == "🃏 シングルカード" else "未開封BOX"
        default_condition = "A (美品)" if reg_mode == "🃏 シングルカード" else "未開封(シュリンク付)"
        
        with st.form("register_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                name = st.text_input("商品名", value=initial_name)
                default_model = ""
                model_num = st.text_input("型番/管理コード", value=default_model, placeholder="手動入力")
                category = st.selectbox("種類", ["シングルカード", "未開封BOX", "サプライ", "その他"], index=["シングルカード", "未開封BOX", "サプライ", "その他"].index(default_category))
                condition = st.selectbox("状態", ["S (完美品)", "A (美品)", "B (傷有)", "C (難あり)", "未開封(シュリンク付)", "未開封(シュリンク無)"], index=1)
                psa_grade = st.selectbox("PSAグレード", ["未鑑定", "10", "9", "その他"], index=0)
                psa_num = st.text_input("PSA証明番号 (Cert #)", placeholder="例: 12345678")
            with col2:
                quantity = st.number_input("在庫数 (個)", min_value=1, value=1, step=1, help="仕入れ値や売値は「1個あたり」の金額を入力してください")
                cost = st.number_input("仕入れ値 (1個あたり)", min_value=0, step=100)
                c_p1, c_p2 = st.columns(2)
                with c_p1: ref_sales = st.number_input("参考販売価格 (1個あたり)", value=initial_sales, step=100)
                with c_p2: ref_buyback = st.number_input("参考買取価格 (1個あたり)", value=0, step=100)
                target_price = st.number_input("想定売値 (1個あたり)", value=initial_sales, step=100)
                location = st.text_input("保管場所", placeholder="例：防湿庫A")
            
            submitted = st.form_submit_button("登録する", use_container_width=True)
            if submitted and name:
                new_data = pd.DataFrame({
                    'ID': [str(uuid.uuid4())[:8]], '商品名': [name], '型番': [model_num],
                    '種類': [category], '状態': [condition], 'PSAグレード': [psa_grade],
                    '仕入れ日': [datetime.now().strftime('%Y-%m-%d')],
                    '仕入れ値': [cost], '想定売値': [target_price], '参考販売': [ref_sales], '参考買取': [ref_buyback], 
                    '保管場所': [location], 'ステータス': ['在庫あり'], 'PSA番号': [str(psa_num)],
                    '在庫数': [quantity]
                })
                if not df.empty: df = pd.concat([df, new_data], ignore_index=True)
                else: df = new_data
                save_data(df)
                st.session_state['selected_item'] = None
                st.session_state['search_candidates'] = []
                st.success(f"「{name}」を {quantity}個 登録しました！")

# ==========================================
# 2. 在庫一覧・編集画面
# ==========================================
elif menu == "📊 在庫一覧・編集":
    st.header("在庫リスト")
    if not df.empty:
        col_filter1, col_filter2 = st.columns([1, 2])
        with col_filter1:
            is_mobile_view = st.toggle("📱 スマホモード（列を絞る）", value=False)
        with col_filter2:
            all_categories = list(df['種類'].unique()) if '種類' in df.columns else []
            selected_categories = st.multiselect("📂 種類で絞り込み (未選択で全表示)", all_categories, default=[])
        
        search_query = st.text_input("🔍 在庫を検索", placeholder="商品名、PSA番号、型番などで検索...")
        
        # フィルタリング
        df_display = df.copy()
        if selected_categories:
            df_display = df_display[df_display['種類'].isin(selected_categories)]
        if search_query:
            mask = df_display.astype(str).apply(lambda x: x.str.contains(search_query, case=False, na=False)).any(axis=1)
            df_display = df_display[mask]

        # 【選択用セレクトボックスの作成】
        # "選択"列は削除して、代わりにこのセレクトボックスを使う
        st.write("▼ 詳細を見たい商品を選択してください")
        
        # セレクトボックス用に分かりやすい名前リストを作成
        # 商品名 + ID（重複防止）の辞書を作る
        select_options = {}
        for idx, row in df_display.iterrows():
            # 表示名: 商品名 [IDの一部]
            label = f"{row['商品名']} (ID:{row['ID']})"
            select_options[label] = row['ID']
        
        selected_label = st.selectbox(
            "👉 商品を選択", 
            options=list(select_options.keys()), 
            index=None, 
            placeholder="選択または入力..."
        )

        # 削除用列の追加
        df_display.insert(0, "削除", False)
        
        def make_psa_url(num):
            if pd.notna(num) and str(num).strip() != "":
                clean_num = re.sub(r'[^0-9]', '', str(num))
                if clean_num: return f"https://www.psacard.com/cert/{clean_num}"
            return None
        df_display["PSAリンク"] = df_display["PSA番号"].apply(make_psa_url)

        all_column_config = {
            "削除": st.column_config.CheckboxColumn("削除", default=False),
            "在庫数": st.column_config.NumberColumn("在庫数", format="%d個", min_value=0),
            "仕入れ値": st.column_config.NumberColumn(format="¥%d"),
            "想定売値": st.column_config.NumberColumn(format="¥%d"),
            "参考販売": st.column_config.NumberColumn(format="¥%d"),
            "参考買取": st.column_config.NumberColumn(format="¥%d"),
            "PSA番号": st.column_config.TextColumn(help="8桁の証明番号"),
            "PSAリンク": st.column_config.LinkColumn("PSA確認", display_text="証明書"),
            "ステータス": st.column_config.SelectboxColumn(options=["在庫あり", "出品中", "売却済み", "PSA提出中"], required=True)
        }

        if is_mobile_view:
            target_cols = ["削除", "商品名", "在庫数", "ステータス", "想定売値", "PSAリンク", "ID"]
            df_display = df_display[[c for c in target_cols if c in df_display.columns]]
            st.info("💡 スマホモード: 重要な列のみ表示しています。")

        # 編集エリア
        edited_df = st.data_editor(
            df_display, num_rows="dynamic",
            column_config=all_column_config,
            key="inventory_editor",
            hide_index=True,
            use_container_width=True
        )

        # 【詳細表示エリア】
        if selected_label:
            # 選択されたIDを取得
            target_id = select_options[selected_label]
            # 編集後のデータから対象行を取得
            target_row = edited_df[edited_df['ID'] == target_id]
            
            if not target_row.empty:
                row_data = target_row.iloc[0]
                raw_name = row_data['商品名']
                clean_name = clean_product_name(raw_name)
                
                st.divider()
                st.markdown(f"### 🔍 詳細アクション: **{raw_name}**")
                
                c1, c2 = st.columns(2)
                with c1:
                    mercari_url = f"https://jp.mercari.com/search?keyword={quote(clean_name)}&status=on_sale"
                    st.link_button("🔴 メルカリで相場", mercari_url, use_container_width=True)
                with c2:
                    rush_url = f"https://cardrush.media/pokemon/buying_prices?displayMode=%E3%83%AA%E3%82%B9%E3%83%88&name={quote(clean_name)}&sort%5Bkey%5D=amount&sort%5Border%5D=desc"
                    st.link_button("🔵 ラッシュ買取表", rush_url, use_container_width=True)
                
                c3, c4 = st.columns(2)
                with c3:
                    yahoo_url = f"https://paypayfleamarket.yahoo.co.jp/search/{quote(clean_name)}?open=1"
                    st.link_button("🟡 Yahoo!フリマ", yahoo_url, use_container_width=True)
                with c4:
                    clove_url = f"https://clove.jp/search?q={quote(clean_name)}"
                    st.link_button("⚫ Cloveで見る", clove_url, use_container_width=True)
                st.divider()

        col_act1, col_act2 = st.columns([1, 1])
        with col_act1:
            if st.button("🗑️ チェックした項目を削除", use_container_width=True):
                if '削除' in edited_df.columns:
                    ids_to_delete = edited_df[edited_df['削除']]['ID'].tolist()
                    if ids_to_delete:
                        df_new = df[~df['ID'].isin(ids_to_delete)]
                        save_data(df_new)
                        st.success(f"{len(ids_to_delete)} 件削除しました。")
                        st.rerun()
                    else: st.info("削除チェックがありません。")
        with col_act2:
            if st.button("🔄 表示中の販売価格を更新", use_container_width=True):
                ids_to_update = df_display['ID'].tolist()
                if not ids_to_update: st.warning("データがありません。")
                else:
                    bar = st.progress(0); txt = st.empty()
                    for i, rid in enumerate(ids_to_update):
                        txt.text(f"更新中... ({i+1}/{len(ids_to_update)})")
                        bar.progress((i + 1) / len(ids_to_update))
                        row = df[df['ID'] == rid].iloc[0]
                        keyword = row['商品名']
                        try:
                            results = search_card_rush(keyword)
                            if results:
                                df.loc[df['ID'] == rid, '参考販売'] = results[0]['price']
                            time.sleep(1)
                        except: pass
                    save_data(df)
                    txt.text("完了！"); time.sleep(1); st.rerun()

        cols_to_save = [c for c in edited_df.columns if c not in ['削除', 'PSAリンク']]
        edited_content = edited_df[cols_to_save]
        
        if not edited_content.empty:
            df.set_index('ID', inplace=True)
            edited_content.set_index('ID', inplace=True)
            df.update(edited_content)
            df.reset_index(inplace=True)
            save_data(df)
    else: st.info("データがありません。")

# ==========================================
# 3. 収支分析画面
# ==========================================
elif menu == "💰 収支分析":
    st.header("資産状況ダッシュボード")
    if not df.empty:
        stock_df = df[df['ステータス'] != '売却済み']
        col1, col2, col3 = st.columns(3)
        total_items = stock_df['在庫数'].sum()
        total_cost = (stock_df['仕入れ値'] * stock_df['在庫数']).sum()
        total_target = (stock_df['想定売値'] * stock_df['在庫数']).sum()
        total_market_sales = (stock_df['参考販売'] * stock_df['在庫数']).sum()

        col1.metric("📦 在庫総数", f"{total_items:,} 個")
        col2.metric("💰 仕入れ総額", f"¥{total_cost:,.0f}")
        col3.metric("🏷️ 想定売上総額", f"¥{total_target:,.0f}")
        st.divider()
        st.subheader("📊 市場価値")
        st.metric("現在の販売相場総額", f"¥{total_market_sales:,.0f}", delta=f"{total_market_sales - total_cost:,.0f} (差益)" if total_cost > 0 else None)
        st.divider()
        st.subheader("在庫の内訳")
        if not stock_df.empty:
            chart_data = stock_df.groupby('種類')['在庫数'].sum()
            st.dataframe(chart_data, use_container_width=True)
    else: st.info("データがありません。")
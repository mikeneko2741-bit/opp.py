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
# Googleスプレッドシートの設定
JSON_KEY_FILE = 'secrets.json'  # 鍵ファイルの名前
SPREADSHEET_NAME = 'ポケカ在庫管理DB'  # スプレッドシートの名前

# エキスパンションリスト (完全版)
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
# データ読み書き機能 (Google Sheets対応・型エラー修正版)
# ---------------------------------------------------------
@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"認証エラー: {JSON_KEY_FILE} が見つからないか、設定が間違っています。\n詳細: {e}")
        return None

def get_sheet():
    client = get_gspread_client()
    if client:
        try:
            sheet = client.open(SPREADSHEET_NAME).sheet1
            return sheet
        except gspread.exceptions.SpreadsheetNotFound:
            st.error(f"スプレッドシート「{SPREADSHEET_NAME}」が見つかりません。共有設定を確認してください。")
            return None
    return None

def load_data():
    sheet = get_sheet()
    if sheet:
        # gspread_dataframeを使ってデータフレームとして取得
        df = get_as_dataframe(sheet, evaluate_formulas=True)
        
        # 空行を削除（IDがない行はデータなしとみなす）
        df = df.dropna(subset=['ID'])
        df = df[df['ID'] != ''] # 空文字も除外
        
        # 欠損値の穴埋めと型変換
        cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', 
                '仕入れ値', '想定売値', '参考販売', '参考買取', '保管場所', 'ステータス', 'PSA番号']
        
        # スプレッドシートに列が足りない場合の補完
        for col in cols:
            if col not in df.columns:
                df[col] = ""

        # 数値列の処理
        num_cols = ['仕入れ値', '想定売値', '参考販売', '参考買取']
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 文字列列の処理 (【重要】PSA番号を強制的に文字列にする)
        str_cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', '保管場所', 'ステータス', 'PSA番号']
        for col in str_cols:
            # NaNを空文字に変換してから文字列型にする
            df[col] = df[col].fillna("").astype(str)
            
            # PSA番号などが「1234.0」のように少数になっていたら「.0」を消す
            if col == 'PSA番号':
                df[col] = df[col].apply(lambda x: x.replace(".0", "") if x.endswith(".0") else x)

        return df
    else:
        # 読み込めない場合は空のDFを返す
        return pd.DataFrame(columns=[
            'ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', 
            '仕入れ値', '想定売値', '参考販売', '参考買取', '保管場所', 'ステータス', 'PSA番号'
        ])

def save_data(df):
    sheet = get_sheet()
    if sheet:
        # 保存するカラムの順序を固定
        save_cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', 
                     '仕入れ値', '想定売値', '参考販売', '参考買取', '保管場所', 'ステータス', 'PSA番号']
        
        # データフレームを整形
        df_to_save = df.copy()
        for col in save_cols:
            if col not in df_to_save.columns:
                df_to_save[col] = ""
        
        # 保存時も念のため文字列型を強制
        str_cols = ['ID', '商品名', '型番', '種類', '状態', 'PSAグレード', '仕入れ日', '保管場所', 'ステータス', 'PSA番号']
        for col in str_cols:
            df_to_save[col] = df_to_save[col].astype(str)

        df_to_save = df_to_save[save_cols]
        
        # スプレッドシートをクリアして書き込み
        sheet.clear()
        set_with_dataframe(sheet, df_to_save)

# ---------------------------------------------------------
# スクレイピング機能（販売価格のみ・安定版）
# ---------------------------------------------------------
def search_card_rush(keyword):
    found = False; name = ""; price = 0
    try:
        base_url = "https://www.cardrush-pokemon.jp"
        search_url = f"{base_url}/product-list?keyword={quote(keyword)}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(search_url, headers=headers, timeout=5)
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.select('.item_box')
        if items:
            item = items[0]
            name_tag = item.select_one('.item_name')
            name = name_tag.text.strip() if name_tag else "取得不可"
            price_tag = item.select_one('.figure')
            if price_tag:
                nums = re.findall(r'\d+', price_tag.text.replace(',', ''))
                if nums: price = int(nums[0])
            if price > 0: found = True
        return {"found": found, "name": name, "price": price}
    except Exception:
        return {"found": False, "name": "", "price": 0}

# ---------------------------------------------------------
# アプリ画面の構築
# ---------------------------------------------------------
st.set_page_config(page_title="ポケカ在庫管理", layout="wide")
st.title("🎴 ポケカ在庫・収支管理システム (Cloud版)")

# データのロード (Google Sheetsから)
df = load_data()

menu = st.sidebar.radio("メニュー", ["📦 在庫登録", "📊 在庫一覧・編集", "💰 収支分析"])

# ==========================================
# 1. 在庫登録画面
# ==========================================
if menu == "📦 在庫登録":
    st.header("新規在庫の登録")
    
    reg_mode = st.radio("登録モード", ["🃏 シングルカード", "📦 未開封BOX"], horizontal=True)
    
    st.subheader("① 商品検索 (販売価格)")
    col_search1, col_search2, col_search3 = st.columns([2, 1, 1])
    
    selected_exp_name = "その他"
    expansion_code = ""
    card_number = ""
    
    with col_search1:
        selected_exp_name = st.selectbox("エキスパンション", list(EXPANSION_LIST.keys()), index=1)
        expansion_code = EXPANSION_LIST[selected_exp_name]
    
    with col_search2:
        if reg_mode == "🃏 シングルカード":
            card_number = st.text_input("カード番号", placeholder="例: 100")
        else:
            st.info("BOX名で検索します")
            card_number = "" 
    
    if 'search_result' not in st.session_state: st.session_state['search_result'] = None

    with col_search3:
        st.write("") 
        st.write("")
        if st.button("🔍 情報を取得"):
            search_keyword = ""
            if reg_mode == "🃏 シングルカード":
                if expansion_code and card_number: search_keyword = f"{expansion_code} {card_number}"
                else: st.warning("パックと番号を入力してください")
            else:
                if selected_exp_name and selected_exp_name != "選択してください":
                    exp_name_only = selected_exp_name.split("(")[0].strip()
                    search_keyword = f"{exp_name_only} BOX"
                else: st.warning("エキスパンションを選択してください")
            
            if search_keyword:
                with st.spinner('カードラッシュから情報を取得中...'):
                    result = search_card_rush(search_keyword)
                    st.session_state['search_result'] = result
                    if not result['found']: st.error("見つかりませんでした。")

    st.divider()

    initial_name = ""
    initial_sales = 0
    default_category = "シングルカード" if reg_mode == "🃏 シングルカード" else "未開封BOX"
    default_condition = "A (美品)" if reg_mode == "🃏 シングルカード" else "未開封(シュリンク付)"
    
    if st.session_state['search_result']:
        res = st.session_state['search_result']
        if res['found']:
            initial_name = res['name']
            initial_sales = res['price']
            st.success(f"ヒット: {initial_name}")
            st.info(f"🛒 現在の販売相場: ¥{initial_sales:,}")
        else:
            st.warning("自動取得できませんでした。手動で入力してください。")
    
    with st.form("register_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("商品名", value=initial_name)
            
            default_model = ""
            if reg_mode == "🃏 シングルカード":
                if expansion_code and card_number: default_model = f"{expansion_code}-{card_number}"
            else:
                if expansion_code: default_model = f"{expansion_code}-BOX"
                    
            model_num = st.text_input("型番/管理コード", value=default_model)
            category = st.selectbox("種類", ["シングルカード", "未開封BOX", "サプライ", "その他"], index=["シングルカード", "未開封BOX", "サプライ", "その他"].index(default_category))
            condition = st.selectbox("状態", ["S (完美品)", "A (美品)", "B (傷有)", "C (難あり)", "未開封(シュリンク付)", "未開封(シュリンク無)"], index=1)
            psa_grade = st.selectbox("PSAグレード", ["未鑑定", "10", "9", "その他"], index=0)
            psa_num = st.text_input("PSA証明番号 (Cert #)", placeholder="例: 12345678")
            
        with col2:
            cost = st.number_input("仕入れ値 (円)", min_value=0, step=100)
            c_p1, c_p2 = st.columns(2)
            with c_p1: ref_sales = st.number_input("参考販売価格 (円)", value=initial_sales, step=100)
            with c_p2: ref_buyback = st.number_input("参考買取価格 (手動)", value=0, step=100)
            target_price = st.number_input("想定売値 (円)", value=initial_sales, step=100)
            location = st.text_input("保管場所", placeholder="例：防湿庫A")
        
        submitted = st.form_submit_button("登録する")
        
        if submitted and name:
            new_data = pd.DataFrame({
                'ID': [str(uuid.uuid4())[:8]], '商品名': [name], '型番': [model_num],
                '種類': [category], '状態': [condition], 'PSAグレード': [psa_grade],
                '仕入れ日': [datetime.now().strftime('%Y-%m-%d')],
                '仕入れ値': [cost], '想定売値': [target_price], '参考販売': [ref_sales], '参考買取': [ref_buyback], 
                '保管場所': [location], 'ステータス': ['在庫あり'], 'PSA番号': [str(psa_num)]
            })
            if not df.empty: df = pd.concat([df, new_data], ignore_index=True)
            else: df = new_data
            
            save_data(df) # スプレッドシートに保存
            st.session_state['search_result'] = None
            st.success(f"「{name}」を登録しました！")

# ==========================================
# 2. 在庫一覧・編集画面
# ==========================================
elif menu == "📊 在庫一覧・編集":
    st.header("在庫リスト")
    
    if not df.empty:
        search_query = st.text_input("🔍 在庫を検索", placeholder="商品名、PSA番号、型番などで検索...")
        if search_query:
            mask = df.astype(str).apply(lambda x: x.str.contains(search_query, case=False, na=False)).any(axis=1)
            df_display = df[mask].copy()
        else:
            df_display = df.copy()

        df_display.insert(0, "削除", False)
        
        def make_psa_url(num):
            if pd.notna(num) and str(num).strip() != "":
                clean_num = re.sub(r'[^0-9]', '', str(num))
                if clean_num: return f"https://www.psacard.com/cert/{clean_num}"
            return None
        
        df_display["PSAリンク"] = df_display["PSA番号"].apply(make_psa_url)

        def make_rush_media_url(name):
            if pd.notna(name) and str(name).strip() != "":
                clean_name = re.sub(r'【.*?】', '', str(name))
                clean_name = re.sub(r'\[.*?\]', '', clean_name)
                clean_name = re.sub(r'\(.*?\)', '', clean_name)
                clean_name = re.sub(r'\{.*?\}', '', clean_name)
                clean_name = re.sub(r'[A-Za-z0-9]+-[A-Za-z0-9]+', '', clean_name)
                clean_name = re.sub(r'[0-9]{3}/[0-9]{3}', '', clean_name)
                clean_name = clean_name.strip()
                if clean_name:
                    encoded_name = quote(clean_name)
                    return f"https://cardrush.media/pokemon/buying_prices?displayMode=%E3%83%AA%E3%82%B9%E3%83%88&name={encoded_name}&sort%5Bkey%5D=amount&sort%5Border%5D=desc"
            return None

        df_display["RushMediaリンク"] = df_display["商品名"].apply(make_rush_media_url)

        edited_df = st.data_editor(
            df_display,
            num_rows="dynamic",
            column_config={
                "削除": st.column_config.CheckboxColumn("削除", default=False),
                "仕入れ値": st.column_config.NumberColumn(format="¥%d"),
                "想定売値": st.column_config.NumberColumn(format="¥%d"),
                "参考販売": st.column_config.NumberColumn(format="¥%d"),
                "参考買取": st.column_config.NumberColumn(format="¥%d"),
                "PSA番号": st.column_config.TextColumn(help="8桁の証明番号"),
                "PSAリンク": st.column_config.LinkColumn("PSA確認", display_text="証明書を見る"),
                "RushMediaリンク": st.column_config.LinkColumn("ラッシュメディア", display_text="買取相場"),
                "ステータス": st.column_config.SelectboxColumn(options=["在庫あり", "出品中", "売却済み", "PSA提出中"], required=True)
            },
            hide_index=True,
            key="inventory_editor"
        )

        col_act1, col_act2 = st.columns([1, 1])
        
        with col_act1:
            if st.button("🗑️ チェックした項目を削除"):
                ids_to_delete = edited_df[edited_df['削除']]['ID'].tolist()
                if ids_to_delete:
                    # 削除対象IDを除外したDFを作成
                    df_new = df[~df['ID'].isin(ids_to_delete)]
                    save_data(df_new)
                    st.success(f"{len(ids_to_delete)} 件削除しました。")
                    st.rerun()
                else: st.info("削除チェックがありません。")

        with col_act2:
            if st.button("🔄 表示中の販売価格を更新"):
                ids_to_update = df_display['ID'].tolist()
                if not ids_to_update: st.warning("データがありません。")
                else:
                    bar = st.progress(0); txt = st.empty()
                    for i, rid in enumerate(ids_to_update):
                        txt.text(f"更新中... ({i+1}/{len(ids_to_update)})")
                        bar.progress((i + 1) / len(ids_to_update))
                        row = df[df['ID'] == rid].iloc[0]
                        model_num = str(row['型番'])
                        keyword = ""
                        if "-BOX" in model_num:
                            if "BOX" in row['商品名']: keyword = row['商品名']
                        elif "-" in model_num:
                            try:
                                ec, cn = model_num.split("-", 1)
                                keyword = f"{ec} {cn}"
                            except: pass
                        if keyword:
                            try:
                                res = search_card_rush(keyword)
                                if res["found"]:
                                    df.loc[df['ID'] == rid, '参考販売'] = res['price']
                                time.sleep(1)
                            except: pass
                    save_data(df)
                    txt.text("完了！")
                    time.sleep(1)
                    st.rerun()

        # 自動保存 (変更があればスプレッドシート更新)
        cols_to_save = [c for c in edited_df.columns if c not in ['削除', 'PSAリンク', 'RushMediaリンク']]
        edited_content = edited_df[cols_to_save]
        
        if not edited_content.empty:
            df.set_index('ID', inplace=True)
            edited_content.set_index('ID', inplace=True)
            df.update(edited_content)
            df.reset_index(inplace=True)
            save_data(df) # ここでスプレッドシートに書き込み

    else:
        st.info("データがありません。")

# ==========================================
# 3. 収支分析画面
# ==========================================
elif menu == "💰 収支分析":
    st.header("資産状況ダッシュボード")
    if not df.empty:
        stock_df = df[df['ステータス'] != '売却済み']
        
        col1, col2, col3 = st.columns(3)
        total_cost = stock_df['仕入れ値'].sum()
        total_target = stock_df['想定売値'].sum()
        total_market_sales = stock_df['参考販売'].sum()
        
        col1.metric("📦 在庫総数", f"{len(stock_df)} 点")
        col2.metric("💰 仕入れ総額", f"¥{total_cost:,.0f}")
        col3.metric("🏷️ 想定売上総額", f"¥{total_target:,.0f}")
        
        st.divider()
        st.subheader("📊 市場価値")
        st.metric("現在の販売相場総額", f"¥{total_market_sales:,.0f}", 
                  delta=f"{total_market_sales - total_cost:,.0f} (差益)" if total_cost > 0 else None)

        st.divider()
        st.subheader("在庫の内訳")
        if not stock_df.empty:
            chart_data = stock_df['種類'].value_counts()
            st.dataframe(chart_data, use_container_width=True)
    else:
        st.info("データがありません。")

# --- コードはここで終了です ---
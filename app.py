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
SPREADSHEET_NAME = 'ぽっけぇ〜道_システムv3' # 新しいスプレッドシート名

# 新しいシート名の定義
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
            st.error("認証キーが見つかりません。")
            return None
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"認証エラーが発生しました。\n詳細: {e}")
        return None

def get_spreadsheet():
    client = get_gspread_client()
    if client:
        try:
            return client.open(SPREADSHEET_NAME)
        except gspread.exceptions.SpreadsheetNotFound:
            st.error(f"⚠️ 新しいスプレッドシート「{SPREADSHEET_NAME}」が見つかりません。名前が完全に一致しているか、サービスアカウントに共有されているか確認してください。")
            return None
    return None

def check_and_init_sheets():
    sh = get_spreadsheet()
    if not sh: return None, None, None

    # 1. 在庫DBの初期化 (v3.0でスッキリさせました)
    try:
        ws_inv = sh.worksheet(SHEET_INVENTORY)
    except:
        ws_inv = sh.add_worksheet(title=SHEET_INVENTORY, rows=1000, cols=15)
        ws_inv.append_row(['ID', '商品名', '種類', '状態_PSA', '仕入日', '原価', '参考相場', '在庫数', '仕入元', 'ステータス'])

    # 2. 仕入帳の初期化
    try:
        ws_pur = sh.worksheet(SHEET_PURCHASE)
    except:
        ws_pur = sh.add_worksheet(title=SHEET_PURCHASE, rows=1000, cols=10)
        ws_pur.append_row(['ID', '仕入日', '仕入名目', '支払総額', '仕入先', '備考', '登録日時'])

    # 3. 売上帳の初期化 (v3.0で手数料・経費の列を追加)
    try:
        ws_sales = sh.worksheet(SHEET_SALES)
    except:
        ws_sales = sh.add_worksheet(title=SHEET_SALES, rows=1000, cols=12)
        ws_sales.append_row(['ID', '売却日', '商品名', '売却数', '売上額', '手数料', '経費_送料', '純利益', '販路', '備考', '登録日時'])

    # デフォルトの「シート1」があれば削除（エラー無視）
    try:
        sh.del_worksheet(sh.worksheet("シート1"))
    except:
        pass

    return ws_inv, ws_pur, ws_sales

# ---------------------------------------------------------
# 🖥️ アプリ画面 (v3.0 メニュー構造)
# ---------------------------------------------------------
st.set_page_config(page_title="ぽっけぇ〜道 システム", layout="wide")
st.title("🎴 ぽっけぇ〜道 管理システム v3.0")

# DB接続チェックと初期化の実行
ws_inv, ws_pur, ws_sales = check_and_init_sheets()

if ws_inv:
    st.sidebar.success("✅ データベース接続OK")
else:
    st.sidebar.error("❌ データベース未接続")

# 新しい4つのメニュー
menu = st.sidebar.radio(
    "【作業メニュー】", 
    ["📦 スピード仕入・解体", "📊 在庫・PSA管理", "🛍️ オリパ工場", "📖 帳簿・分析"]
)

if menu == "📦 スピード仕入・解体":
    st.header("📦 スピード仕入・福袋解体")
    st.info("💡 ここに「カート形式」で福袋の中身やまとめ買い商品を連続登録し、原価を自動計算する機能を実装します。（次回アップデート予定）")

elif menu == "📊 在庫・PSA管理":
    st.header("📊 在庫・PSA管理")
    st.info("💡 ここに「素材」「PSA」「未開封BOX」を分けて表示し、相場確認やPSA費用の後乗せができる機能を実装します。（次回アップデート予定）")

elif menu == "🛍️ オリパ工場":
    st.header("🛍️ オリパ工場")
    st.info("💡 ここに在庫からカードを引き落とし、送料や梱包費を個別に設定してオリパをセット化する機能を実装します。（次回アップデート予定）")

elif menu == "📖 帳簿・分析":
    st.header("📖 帳簿・分析")
    st.info("💡 ここにBASEやメルカリの手数料を自動計算した「真の純利益」や、資産状況のダッシュボードを実装します。（次回アップデート予定）")
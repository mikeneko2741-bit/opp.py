import urllib.request
from urllib.parse import quote
import json

# スニダンで検索するテストキーワード
keyword = "ピカチュウ PSA10"
encoded = quote(keyword)
url = f"https://snkrdunk.com/search/result?keyword={encoded}"

# 人間のブラウザを偽装する設定
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://www.google.com/"
}

print("====================================")
print("🚀 スニダンへ突撃を開始します...")
print("====================================")

req = urllib.request.Request(url, headers=headers)
try:
    with urllib.request.urlopen(req, timeout=10) as response:
        html = response.read().decode('utf-8')
        print("\n🎉 大成功！スニダンの壁を突破しました！")
        print(f"ステータス: {response.getcode()}")
        print("\n【取得したデータの一部】")
        print(html[:300]) # 冒頭だけ表示
except Exception as e:
    print("\n❌ 失敗... やはり弾かれました。")
    print(f"エラー詳細: {e}")

print("\n====================================")
input("エンターキーを押すとこの画面を閉じます...")
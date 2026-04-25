@echo off
chcp 65001 >nul
echo ====================================
echo 🤖 ぽっけぇ〜道 スニダン自動巡回ロボ
echo ====================================
python snkrdunk_robot.py
echo.
echo 処理が完了しました。5秒後に画面を閉じます...
timeout /t 5 >nul
exit
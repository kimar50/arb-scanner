#!/bin/bash
# Обновление ARB Scanner до версии 2.
# Запускать на сервере:  bash deploy.sh
set -e

APP=/opt/arb-scanner
echo "=============================================="
echo "  ARB SCANNER — обновление до v2"
echo "=============================================="

cd $APP

echo ""
echo "[1/5] Забираем новую версию из GitHub..."
git pull

echo ""
echo "[2/5] Проверяем зависимости..."
$APP/venv/bin/pip install --quiet --upgrade aiohttp

echo ""
echo "[3/5] Создаём config.json, если его ещё нет..."
if [ ! -f $APP/config.json ]; then
cat > $APP/config.json <<'JSON'
{
  "google_client_id": "",
  "google_client_secret": "",
  "secret_key": "",
  "admin_emails": [],
  "usdt_trc20": "",
  "telegram_support": "",
  "refresh_seconds": 8,
  "free_delay_seconds": 90
}
JSON
  echo "    создан $APP/config.json — заполните его позже"
else
  echo "    уже есть, не трогаем"
fi

echo ""
echo "[4/5] Переключаем сервис на server.py..."
cat > /etc/systemd/system/arb-scanner.service <<'UNIT'
[Unit]
Description=ARB Scanner
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/arb-scanner
ExecStart=/opt/arb-scanner/venv/bin/python /opt/arb-scanner/server.py
Restart=always
RestartSec=5
Environment=PORT=8765

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl restart arb-scanner

echo ""
echo "[5/5] Проверяем..."
sleep 3
systemctl --no-pager status arb-scanner | head -12

echo ""
echo "=============================================="
echo "  Готово. Открывайте http://31.77.148.207"
echo ""
echo "  Пока config.json пустой — вход отключён,"
echo "  сайт открыт всем и работает как раньше."
echo "=============================================="

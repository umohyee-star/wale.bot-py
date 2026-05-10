"""
🐋 Binance Whale Tracker — النسخة الاحترافية
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- يراقب كل العملات على Binance عبر WebSocket
- تنبيه فوري لصفقات الحيتان
- كشف تراكم الشراء (Pump Detection)
- إحصائيات آخر ساعة / 4 ساعات / 24 ساعة
- أوامر VIP من Telegram

التثبيت:
    pip install websocket-client requests

الإعداد:
    غيّر BOT_TOKEN و CHAT_ID بالقيم بتاعتك
"""

import json
import time
import threading
import requests
import websocket
from datetime import datetime
from collections import defaultdict, deque


# ══════════════════════════════════════════════
#  ⚙️  الإعدادات
# ══════════════════════════════════════════════
BOT_TOKEN = "8487052557:AAG4_lCni0grvefadbNaONgqxRjtmaum3F0"
CHAT_ID   = "6914157653"
MIN_USD     = 20_000
VIP_MIN_USD = 20_000

# حدود التراكم حسب حجم العملة
PUMP_WINDOW_SEC = 300   # 5 دقائق

BIG_COINS    = {"BTC", "ETH"}
MID_COINS    = {"SOL", "BNB", "XRP", "ADA", "AVAX", "DOT", "LINK", "LTC", "TRX"}

PUMP_LIMIT_BIG  = 500_000   # BTC / ETH
PUMP_LIMIT_MID  = 100_000   # عملات متوسطة
PUMP_LIMIT_SMALL = 30_000   # عملات صغيرة

# العملات الثابتة
STABLE_PAIRS = {
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDPUSDT",
    "FDUSDUSDT", "DAIUSDT", "EURUSDT", "USDTUSDT"
}

# ══════════════════════════════════════════════
#  بيانات مشتركة
# ══════════════════════════════════════════════

prices      = {}
prices_lock = threading.Lock()

vip_coins = set()
vip_lock  = threading.Lock()

alert_count = 0
alert_lock  = threading.Lock()

# سجل الصفقات لكل عملة — deque of (timestamp, side, usd_value)
trade_log      = defaultdict(lambda: deque())
trade_log_lock = threading.Lock()

# منع تنبيه Pump المكرر — آخر وقت تنبيه لكل عملة
last_pump_alert = {}
pump_alert_lock = threading.Lock()


# ══════════════════════════════════════════════
#  وقت
# ══════════════════════════════════════════════

def now():
    return datetime.now().strftime("%H:%M:%S")

def ts():
    return time.time()


# ══════════════════════════════════════════════
#  أسعار
# ══════════════════════════════════════════════

def fetch_all_prices():
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price", timeout=10)
        data = r.json()
        with prices_lock:
            for item in data:
                prices[item["symbol"]] = float(item["price"])
        print(f"[{now()}] تم تحديث {len(prices)} سعر")
    except Exception as e:
        print(f"[{now()}] خطأ في جلب الأسعار: {e}")

def price_updater():
    while True:
        fetch_all_prices()
        time.sleep(60)


# ══════════════════════════════════════════════
#  جلب الـ Pairs
# ══════════════════════════════════════════════

def get_all_usdt_pairs():
    try:
        r = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=15)
        data = r.json()
        pairs = [
            s["symbol"].lower()
            for s in data["symbols"]
            if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
        ]
        print(f"[{now()}] تم جلب {len(pairs)} pair من Binance")
        return pairs
    except Exception as e:
        print(f"[{now()}] خطأ في جلب الـ pairs: {e}")
        return []


# ══════════════════════════════════════════════
#  Telegram
# ══════════════════════════════════════════════

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print(f"[{now()}] Telegram error: {r.text}")
    except Exception as e:
        print(f"[{now()}] Telegram exception: {e}")


# ══════════════════════════════════════════════
#  إحصائيات العملة
# ══════════════════════════════════════════════

def get_coin_stats(coin: str) -> dict:
    symbol = coin + "USDT"
    now_ts = ts()
    windows = {
        "1h":  3600,
        "4h":  14400,
        "24h": 86400,
    }
    result = {}
    with trade_log_lock:
        trades = list(trade_log[symbol])

    for label, secs in windows.items():
        cutoff = now_ts - secs
        buy_usd  = sum(t[2] for t in trades if t[0] >= cutoff and t[1] == "BUY")
        sell_usd = sum(t[2] for t in trades if t[0] >= cutoff and t[1] == "SELL")
        total    = buy_usd + sell_usd
        result[label] = {
            "buy":  buy_usd,
            "sell": sell_usd,
            "pct":  (buy_usd / total * 100) if total > 0 else 50,
        }
    return result


def format_stats(coin: str) -> str:
    stats = get_coin_stats(coin)
    with prices_lock:
        price = prices.get(coin + "USDT", 0)

    lines = [f"📊 <b>{coin}/USDT</b>  💵 ${price:,.6f}\n━━━━━━━━━━━━━━━━━━"]
    labels = {"1h": "آخر ساعة", "4h": "آخر 4 ساعات", "24h": "آخر 24 ساعة"}

    for key, label in labels.items():
        d = stats[key]
        pct = d["pct"]
        if pct >= 65:
            sentiment = "🚀 ضغط شراء قوي"
        elif pct >= 55:
            sentiment = "📈 ميل للشراء"
        elif pct <= 35:
            sentiment = "🔴 ضغط بيع قوي"
        elif pct <= 45:
            sentiment = "📉 ميل للبيع"
        else:
            sentiment = "⚖️ محايد"

        lines.append(
            f"\n⏱ <b>{label}</b>\n"
            f"  🟢 شراء: ${d['buy']:>12,.0f}\n"
            f"  🔴 بيع:  ${d['sell']:>12,.0f}\n"
            f"  {sentiment} ({pct:.0f}% شراء)"
        )

    return "\n".join(lines)


# ══════════════════════════════════════════════
#  كشف التراكم (Pump Detection)
# ══════════════════════════════════════════════

def get_pump_limit(coin: str) -> float:
    if coin in BIG_COINS:
        return PUMP_LIMIT_BIG
    elif coin in MID_COINS:
        return PUMP_LIMIT_MID
    else:
        return PUMP_LIMIT_SMALL


def check_pump(coin: str, symbol: str):
    now_ts = ts()
    cutoff  = now_ts - PUMP_WINDOW_SEC

    with trade_log_lock:
        trades = list(trade_log[symbol])

    buy_usd   = sum(t[2] for t in trades if t[0] >= cutoff and t[1] == "BUY")
    sell_usd  = sum(t[2] for t in trades if t[0] >= cutoff and t[1] == "SELL")
    buy_count = sum(1 for t in trades if t[0] >= cutoff and t[1] == "BUY")
    limit     = get_pump_limit(coin)

    if buy_usd < limit:
        return

    # منع تكرار التنبيه في 10 دقائق
    with pump_alert_lock:
        last = last_pump_alert.get(coin, 0)
        if now_ts - last < 600:
            return
        last_pump_alert[coin] = now_ts

    with prices_lock:
        price = prices.get(symbol, 0)

    msg = (
        f"🚨 <b>تراكم شراء مشبوه!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{coin}/USDT</b>\n"
        f"💰 إجمالي الشراء: <b>${buy_usd:,.0f}</b>\n"
        f"🔴 إجمالي البيع:  ${sell_usd:,.0f}\n"
        f"📊 عدد الصفقات: {buy_count}\n"
        f"⏱ خلال: 5 دقائق\n"
        f"💵 السعر الحالي: ${price:,.6f}\n"
        f"🔗 <a href='https://www.binance.com/en/trade/{coin}_USDT'>افتح الشارت</a>"
    )
    threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()
    print(f"[{now()}] 🚨 PUMP DETECTED: {coin} buy=${buy_usd:,.0f}")


# ══════════════════════════════════════════════
#  تنسيق تنبيه الصفقة
# ══════════════════════════════════════════════

def format_alert(symbol, side, qty, price, usd_value, is_vip=False):
    coin   = symbol.replace("USDT", "").upper()
    emoji  = "🟢" if side == "BUY" else "🔴"
    action = "شراء" if side == "BUY" else "بيع"

    if is_vip:
        tag = "⭐️ <b>VIP</b>\n"
    elif usd_value >= 1_000_000:
        tag = "🐋 <b>MEGA WHALE</b>\n"
    elif usd_value >= 500_000:
        tag = "🦈 <b>BIG WHALE</b>\n"
    else:
        tag = ""

    return (
        f"{tag}"
        f"{emoji} <b>{action} — {coin}/USDT</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 القيمة:  <b>${usd_value:,.0f}</b>\n"
        f"📦 الكمية:  {qty:,.4f} {coin}\n"
        f"💵 السعر:   ${price:,.6f}\n"
        f"🕐 الوقت:   {now()}\n"
        f"🔗 <a href='https://www.binance.com/en/trade/{coin}_USDT'>افتح الشارت</a>"
    )


# ══════════════════════════════════════════════
#  WebSocket
# ══════════════════════════════════════════════

def on_message(ws, message):
    global alert_count
    try:
        data = json.loads(message)
        if "data" in data:
            data = data["data"]
        if data.get("e") != "trade":
            return

        symbol = data["s"]
        if symbol in STABLE_PAIRS:
            return

        price     = float(data["p"])
        qty       = float(data["q"])
        side      = "SELL" if data["m"] else "BUY"
        usd_value = qty * price
        coin      = symbol.replace("USDT", "")

        # سجّل الصفقة للإحصائيات والـ pump detection
        with trade_log_lock:
            trade_log[symbol].append((ts(), side, usd_value))
            # امسح القديم (أكتر من 24 ساعة)
            cutoff = ts() - 86400
            while trade_log[symbol] and trade_log[symbol][0][0] < cutoff:
                trade_log[symbol].popleft()

        # فحص pump
        if side == "BUY":
            threading.Thread(target=check_pump, args=(coin, symbol), daemon=True).start()

        # تحقق VIP
        with vip_lock:
            is_vip = coin in vip_coins

        if not is_vip and usd_value < MIN_USD:
            return
        if is_vip and usd_value < VIP_MIN_USD:
            return

        with alert_lock:
            alert_count += 1
            count = alert_count

        print(f"[{now()}] #{count} {'⭐' if is_vip else ''}{side:4s} {coin:10s} ${usd_value:>12,.0f}")

        msg = format_alert(symbol, side, qty, price, usd_value, is_vip)
        threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()

    except Exception as e:
        print(f"[{now()}] on_message error: {e}")


def on_error(ws, error):
    print(f"[{now()}] WebSocket error: {error}")

def on_close(ws, *args):
    print(f"[{now()}] WebSocket closed — reconnecting...")

def on_open(ws):
    print(f"[{now()}] WebSocket connected!")


# ══════════════════════════════════════════════
#  أوامر Telegram
# ══════════════════════════════════════════════

def handle_commands():
    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            r = requests.get(url, params={"offset": last_update_id + 1, "timeout": 30}, timeout=35)
            updates = r.json().get("result", [])

            for update in updates:
                last_update_id = update["update_id"]
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip()
                chat_id = str(msg.get("chat", {}).get("id", ""))

                if chat_id != str(CHAT_ID):
                    continue

                # /vip_add
                if text.startswith("/vip_add"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram("❌ مثال: /vip_add BTC")
                        continue
                    coin = parts[1].upper().replace("USDT", "")
                    with vip_lock:
                        vip_coins.add(coin)
                    send_telegram(f"⭐️ تم إضافة <b>{coin}</b> للـ VIP")

                # /vip_remove
                elif text.startswith("/vip_remove"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram("❌ مثال: /vip_remove BTC")
                        continue
                    coin = parts[1].upper().replace("USDT", "")
                    with vip_lock:
                        vip_coins.discard(coin)
                    send_telegram(f"🗑 تم إزالة <b>{coin}</b> من VIP")

                # /vip_list
                elif text == "/vip_list":
                    with vip_lock:
                        coins = sorted(vip_coins)
                    if coins:
                        send_telegram("⭐️ <b>عملات VIP:</b>\n" + "\n".join(f"• {c}" for c in coins))
                    else:
                        send_telegram("📭 مفيش عملات VIP")

                # /stats
                elif text.startswith("/stats"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram("❌ مثال: /stats BTC")
                        continue
                    coin = parts[1].upper().replace("USDT", "")
                    send_telegram(format_stats(coin))

                # /status
                elif text == "/status":
                    with vip_lock:
                        vip = sorted(vip_coins)
                    with alert_lock:
                        count = alert_count
                    send_telegram(
                        f"✅ <b>البوت شغال</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📊 تنبيهات: <b>{count}</b>\n"
                        f"⭐️ VIP: {', '.join(vip) if vip else 'لا يوجد'}\n"
                        f"💰 الحد الأدنى: ${MIN_USD:,}"
                    )

                # /help
                elif text == "/help":
                    send_telegram(
                        "🐋 <b>أوامر البوت:</b>\n"
                        "━━━━━━━━━━━━━━━━━━\n"
                        "/vip_add BTC — إضافة عملة VIP\n"
                        "/vip_remove BTC — إزالة عملة VIP\n"
                        "/vip_list — عرض عملات VIP\n"
                        "/stats BTC — إحصائيات عملة\n"
                        "/status — حالة البوت\n"
                        "/help — الأوامر"
                    )

        except Exception as e:
            print(f"[{now()}] commands error: {e}")
        time.sleep(2)


# ══════════════════════════════════════════════
#  تقسيم الـ Pairs
# ══════════════════════════════════════════════

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def run_websocket(pairs_chunk, index):
    streams = "/".join([f"{p}@trade" for p in pairs_chunk])
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[{now()}] Connection #{index} exception: {e}")
        time.sleep(5)


# ══════════════════════════════════════════════
#  فحص Telegram
# ══════════════════════════════════════════════

def check_telegram():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMe"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            name = r.json()["result"]["username"]
            print(f"[{now()}] Telegram Bot: @{name}")
            return True
        print(f"[{now()}] Telegram Token غلط")
        return False
    except Exception as e:
        print(f"[{now()}] Telegram error: {e}")
        return False


# ══════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════

def main():
    print("=" * 55)
    print("  🐋 Binance Whale Tracker — النسخة الاحترافية")
    print(f"  الحد الأدنى: ${MIN_USD:,}")
    print("=" * 55)

    if not check_telegram():
        print("تأكد من BOT_TOKEN و CHAT_ID وأعد التشغيل")
        return

    all_pairs = get_all_usdt_pairs()
    if not all_pairs:
        print("مقدرش أجيب الـ pairs.")
        return

    fetch_all_prices()

    threading.Thread(target=price_updater, daemon=True).start()
    threading.Thread(target=handle_commands, daemon=True).start()

    chunks = list(chunk_list(all_pairs, 200))
    print(f"[{now()}] فتح {len(chunks)} connection(s) لـ {len(all_pairs)} pair...")

    send_telegram(
        "🐋 <b>Whale Tracker شغال!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 المنصة: Binance (كل العملات)\n"
        f"🔢 عدد الـ Pairs: <b>{len(all_pairs)}</b>\n"
        f"💰 الحد الأدنى: <b>${MIN_USD:,}</b>\n"
        f"🚨 Pump Detection: ✅ شغال\n"
        f"📈 إحصائيات: /stats BTC\n"
        f"⏰ بدأ في: {now()}"
    )

    threads = []
    for i, chunk in enumerate(chunks, 1):
        t = threading.Thread(target=run_websocket, args=(chunk, i), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.5)

    print(f"[{now()}] كل الـ connections شغالة!\n")

    try:
        while True:
            time.sleep(60)
            print(f"[{now()}] alive | تنبيهات: {alert_count} | pairs: {len(all_pairs)}")
    except KeyboardInterrupt:
        print(f"\n[{now()}] تم إيقاف البوت.")


if __name__ == "__main__":
    main()

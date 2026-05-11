"""
🐋 Binance Whale Tracker v4
━━━━━━━━━━━━━━━━━━━━━━━━━━
- يبعت تنبيه لو اتحققت أي شروط متوفرة
- يوضح الشروط اللي اتحققت والناقصة
- يتجاهل كل الـ stablecoins
- كل فريم مستقل تماماً

التثبيت:
    pip install websocket-client requests
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
WINDOWS = {
    "5m":  300,
    "1h":  3600,
    "4h":  14400,
    "24h": 86400,
}

CONDITIONS = {
    "BTC": {
        "5m":   50_000_000,
        "1h":  150_000_000,
        "4h":  200_000_000,
        "24h": 500_000_000,
    },
    "ETH": {
        "5m":   20_000_000,
        "1h":   80_000_000,
        "4h":  150_000_000,
        "24h": 300_000_000,
    },
    "SOL": {
        "5m":     100_000,
        "1h":     500_000,
        "4h":   2_000_000,
        "24h": 10_000_000,
    },
    "BNB": {
        "5m":     100_000,
        "1h":     500_000,
        "4h":   2_000_000,
        "24h": 10_000_000,
    },
    "XRP": {
        "5m":     100_000,
        "1h":     500_000,
        "4h":   2_000_000,
        "24h": 10_000_000,
    },
    "DEFAULT": {
        "5m":   20_000,
        "1h":   60_000,
        "4h":  150_000,
        "24h": 300_000,
    },
}

# لازم على الأقل شرطين يتحققوا عشان يبعت تنبيه
MIN_CONDITIONS_MET = 2

PUMP_COOLDOWN = 600
VIP_MIN_USD   = 5_000

# كل العملات الثابتة والمستقرة
STABLE_COINS = {
    "USDC", "BUSD", "TUSD", "USDP", "FDUSD",
    "DAI", "USDD", "USDJ", "USDX", "USDT1",
    "EUR", "GBP", "AUD", "AEUR", "BEUR",
    "PAXG", "XAUT",
}

def is_stable(coin: str) -> bool:
    return coin in STABLE_COINS or coin.startswith("USD") or coin.endswith("USD")


# ══════════════════════════════════════════════
#  بيانات مشتركة
# ══════════════════════════════════════════════

prices           = {}
prices_lock      = threading.Lock()

vip_coins        = set()
vip_lock         = threading.Lock()

alert_count      = 0
alert_lock       = threading.Lock()

trade_log        = defaultdict(deque)
trade_log_lock   = threading.Lock()

first_trade_ts   = {}
first_trade_lock = threading.Lock()

last_pump_alert  = {}
pump_alert_lock  = threading.Lock()

bot_start_ts     = time.time()


# ══════════════════════════════════════════════
#  مساعدات
# ══════════════════════════════════════════════

def now():
    return datetime.now().strftime("%H:%M:%S")

def ts():
    return time.time()

def get_conditions(coin):
    return CONDITIONS.get(coin, CONDITIONS["DEFAULT"])


# ══════════════════════════════════════════════
#  أسعار
# ══════════════════════════════════════════════

def fetch_all_prices():
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price", timeout=10)
        with prices_lock:
            for item in r.json():
                prices[item["symbol"]] = float(item["price"])
        print(f"[{now()}] تم تحديث {len(prices)} سعر")
    except Exception as e:
        print(f"[{now()}] خطأ في الأسعار: {e}")

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
        pairs = [
            s["symbol"].lower()
            for s in r.json()["symbols"]
            if s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
            and not is_stable(s["baseAsset"])
        ]
        print(f"[{now()}] تم جلب {len(pairs)} pair")
        return pairs
    except Exception as e:
        print(f"[{now()}] خطأ في الـ pairs: {e}")
        return []


# ══════════════════════════════════════════════
#  Telegram
# ══════════════════════════════════════════════

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
        if r.status_code != 200:
            print(f"[{now()}] Telegram error: {r.text}")
    except Exception as e:
        print(f"[{now()}] Telegram exception: {e}")


# ══════════════════════════════════════════════
#  حساب فريم زمني مستقل
# ══════════════════════════════════════════════

def calc_window(symbol: str, seconds: int) -> dict:
    now_ts = ts()
    start  = now_ts - seconds

    with trade_log_lock:
        trades = list(trade_log[symbol])

    window_trades = [t for t in trades if t[0] >= start]

    buy_usd   = sum(t[2] for t in window_trades if t[1] == "BUY")
    sell_usd  = sum(t[2] for t in window_trades if t[1] == "SELL")
    buy_count = sum(1 for t in window_trades if t[1] == "BUY")
    total     = buy_usd + sell_usd
    pct_buy   = (buy_usd / total * 100) if total > 0 else 0
    pct_sell  = 100 - pct_buy

    with first_trade_lock:
        first = first_trade_ts.get(symbol, now_ts)
    data_age   = now_ts - first
    has_enough = data_age >= seconds

    return {
        "buy": buy_usd, "sell": sell_usd,
        "buy_count": buy_count,
        "pct_buy": pct_buy, "pct_sell": pct_sell,
        "has_enough": has_enough,
        "data_age_min": int(data_age / 60),
    }

def sentiment(pct_buy):
    if pct_buy >= 70: return "🚀 ضغط شراء قوي جداً"
    if pct_buy >= 60: return "📈 ضغط شراء"
    if pct_buy >= 50: return "↗️ ميل للشراء"
    if pct_buy <= 30: return "🔴 ضغط بيع قوي"
    if pct_buy <= 40: return "📉 ميل للبيع"
    return "⚖️ محايد"

def fmt_window(label, d, cond_val):
    if not d["has_enough"]:
        return (
            f"⏱ <b>{label}</b>\n"
            f"  ⏳ جاري جمع البيانات ({d['data_age_min']} دقيقة)\n"
            f"  📋 الشرط: ${cond_val:,.0f}"
        )
    met = "✅" if d["buy"] >= cond_val else "❌"
    return (
        f"⏱ <b>{label}</b>  {met}\n"
        f"  🟢 شراء: ${d['buy']:>14,.0f}  ({d['pct_buy']:.1f}%)\n"
        f"  🔴 بيع:  ${d['sell']:>14,.0f}  ({d['pct_sell']:.1f}%)\n"
        f"  {sentiment(d['pct_buy'])}\n"
        f"  📋 الشرط: ${cond_val:,.0f}"
    )


# ══════════════════════════════════════════════
#  فحص الشروط وإرسال التنبيه
# ══════════════════════════════════════════════

def check_conditions(coin: str, symbol: str):
    now_ts = ts()

    with pump_alert_lock:
        if now_ts - last_pump_alert.get(coin, 0) < PUMP_COOLDOWN:
            return

    cond = get_conditions(coin)

    d5m  = calc_window(symbol, WINDOWS["5m"])
    d1h  = calc_window(symbol, WINDOWS["1h"])
    d4h  = calc_window(symbol, WINDOWS["4h"])
    d24h = calc_window(symbol, WINDOWS["24h"])

    windows_data = [
        ("5m",  d5m,  cond["5m"],  "آخر 5 دقائق"),
        ("1h",  d1h,  cond["1h"],  "آخر ساعة"),
        ("4h",  d4h,  cond["4h"],  "آخر 4 ساعات"),
        ("24h", d24h, cond["24h"], "آخر 24 ساعة"),
    ]

    # عدّ الشروط المتحققة (بيانات كافية + تجاوز الحد)
    met_count = sum(
        1 for _, d, c, _ in windows_data
        if d["has_enough"] and d["buy"] >= c
    )

    # لازم على الأقل MIN_CONDITIONS_MET شروط تتحقق
    if met_count < MIN_CONDITIONS_MET:
        return

    with pump_alert_lock:
        last_pump_alert[coin] = now_ts

    with prices_lock:
        price = prices.get(symbol, 0)

    # اعمل الرسالة
    lines = [
        f"🚨 <b>تراكم شراء — {coin}/USDT</b>",
        f"✅ شروط محققة: {met_count}/4",
        "━━━━━━━━━━━━━━━━━━",
    ]

    for _, d, c, label in windows_data:
        lines.append(fmt_window(label, d, c))
        lines.append("")

    lines += [
        "━━━━━━━━━━━━━━━━━━",
        f"📊 صفقات شراء (5د): {d5m['buy_count']}",
        f"💵 السعر: ${price:,.6f}",
        f"🔗 <a href='https://www.binance.com/en/trade/{coin}_USDT'>افتح الشارت</a>",
    ]

    msg = "\n".join(lines)
    threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()
    print(f"[{now()}] 🚨 {coin} | {met_count}/4 شروط | 5m=${d5m['buy']:,.0f}")


# ══════════════════════════════════════════════
#  تنبيه VIP
# ══════════════════════════════════════════════

def format_vip_alert(symbol, side, qty, price, usd_value):
    coin   = symbol.replace("USDT", "")
    emoji  = "🟢" if side == "BUY" else "🔴"
    action = "شراء" if side == "BUY" else "بيع"
    tag    = (
        "🐋 <b>MEGA WHALE</b>\n" if usd_value >= 1_000_000 else
        "🦈 <b>BIG WHALE</b>\n"  if usd_value >= 500_000   else ""
    )
    return (
        f"⭐️ <b>VIP</b>  {tag}"
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
        coin   = symbol.replace("USDT", "")

        if is_stable(coin):
            return

        price     = float(data["p"])
        qty       = float(data["q"])
        side      = "SELL" if data["m"] else "BUY"
        usd_value = qty * price
        now_ts    = ts()

        # سجّل الصفقة
        with trade_log_lock:
            trade_log[symbol].append((now_ts, side, usd_value))
            cutoff = now_ts - 86400
            while trade_log[symbol] and trade_log[symbol][0][0] < cutoff:
                trade_log[symbol].popleft()

        # سجّل أول صفقة
        with first_trade_lock:
            if symbol not in first_trade_ts:
                first_trade_ts[symbol] = now_ts

        # فحص الشروط
        if side == "BUY":
            threading.Thread(
                target=check_conditions, args=(coin, symbol), daemon=True
            ).start()

        # تنبيه VIP
        with vip_lock:
            is_vip = coin in vip_coins
        if is_vip and usd_value >= VIP_MIN_USD:
            with alert_lock:
                alert_count += 1
            msg = format_vip_alert(symbol, side, qty, price, usd_value)
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
            r = requests.get(
                url,
                params={"offset": last_update_id + 1, "timeout": 30},
                timeout=35
            )
            updates = r.json().get("result", [])

            for update in updates:
                last_update_id = update["update_id"]
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(CHAT_ID):
                    continue

                if text.startswith("/vip_add"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram("❌ مثال: /vip_add BTC")
                        continue
                    coin = parts[1].upper().replace("USDT", "")
                    with vip_lock:
                        vip_coins.add(coin)
                    send_telegram(f"⭐️ تم إضافة <b>{coin}</b> للـ VIP")

                elif text.startswith("/vip_remove"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram("❌ مثال: /vip_remove BTC")
                        continue
                    coin = parts[1].upper().replace("USDT", "")
                    with vip_lock:
                        vip_coins.discard(coin)
                    send_telegram(f"🗑 تم إزالة <b>{coin}</b> من VIP")

                elif text == "/vip_list":
                    with vip_lock:
                        coins = sorted(vip_coins)
                    send_telegram(
                        "⭐️ <b>عملات VIP:</b>\n" + "\n".join(f"• {c}" for c in coins)
                        if coins else "📭 مفيش عملات VIP"
                    )

                elif text.startswith("/stats"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_telegram("❌ مثال: /stats BTC")
                        continue
                    coin   = parts[1].upper().replace("USDT", "")
                    symbol = coin + "USDT"
                    cond   = get_conditions(coin)
                    with prices_lock:
                        price = prices.get(symbol, 0)
                    d5m  = calc_window(symbol, WINDOWS["5m"])
                    d1h  = calc_window(symbol, WINDOWS["1h"])
                    d4h  = calc_window(symbol, WINDOWS["4h"])
                    d24h = calc_window(symbol, WINDOWS["24h"])
                    send_telegram(
                        f"📊 <b>{coin}/USDT</b>  💵 ${price:,.6f}\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"{fmt_window('آخر 5 دقائق', d5m, cond['5m'])}\n\n"
                        f"{fmt_window('آخر ساعة', d1h, cond['1h'])}\n\n"
                        f"{fmt_window('آخر 4 ساعات', d4h, cond['4h'])}\n\n"
                        f"{fmt_window('آخر 24 ساعة', d24h, cond['24h'])}"
                    )

                elif text == "/status":
                    uptime = int((ts() - bot_start_ts) / 60)
                    with vip_lock:
                        vip = sorted(vip_coins)
                    with alert_lock:
                        count = alert_count
                    send_telegram(
                        f"✅ <b>البوت شغال</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⏱ وقت التشغيل: {uptime} دقيقة\n"
                        f"📊 تنبيهات VIP: <b>{count}</b>\n"
                        f"⭐️ VIP: {', '.join(vip) if vip else 'لا يوجد'}\n"
                        f"🚨 Pump Detection: ✅\n"
                        f"📋 الحد الأدنى للتنبيه: {MIN_CONDITIONS_MET}/4 شروط"
                    )

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
#  WebSocket connections
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
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=10
        )
        if r.status_code == 200:
            print(f"[{now()}] Telegram: @{r.json()['result']['username']}")
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
    print("  🐋 Binance Whale Tracker v4")
    print(f"  يبعت لو اتحققت {MIN_CONDITIONS_MET}/4 شروط على الأقل")
    print("=" * 55)

    if not check_telegram():
        print("تأكد من BOT_TOKEN و CHAT_ID")
        return

    all_pairs = get_all_usdt_pairs()
    if not all_pairs:
        return

    fetch_all_prices()
    threading.Thread(target=price_updater, daemon=True).start()
    threading.Thread(target=handle_commands, daemon=True).start()

    chunks = list(chunk_list(all_pairs, 200))
    print(f"[{now()}] فتح {len(chunks)} connection(s) لـ {len(all_pairs)} pair...")

    send_telegram(
        "🐋 <b>Whale Tracker v4 شغال!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Binance — {len(all_pairs)} عملة (بدون stablecoins)\n"
        f"📋 التنبيه يجي لو {MIN_CONDITIONS_MET}/4 شروط اتحققت\n"
        f"⏳ الفريمات الناقصة بيانات بتظهر تلقائي\n"
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
            uptime = int((ts() - bot_start_ts) / 60)
            print(f"[{now()}] alive | uptime: {uptime}m | pairs: {len(all_pairs)}")
    except KeyboardInterrupt:
        print(f"\n[{now()}] تم إيقاف البوت.")


if __name__ == "__main__":
    main()

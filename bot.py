"""
Binance Trading Bot — Discord
Stack: nextcord + python-binance + gspread (Google Sheets)
Deploy: Railway (single worker, no Docker, no Redis, no Postgres)
"""

import os
import json
import base64
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import nextcord
from nextcord.ext import commands, tasks
from binance import AsyncClient
from binance.exceptions import BinanceAPIException
import gspread
from google.oauth2.service_account import Credentials
import pytz

DISCORD_TOKEN          = os.environ["DISCORD_TOKEN"]
DISCORD_CHANNEL_ID     = int(os.environ.get("DISCORD_CHANNEL_ID", "0"))
BINANCE_API_KEY        = os.environ["BINANCE_API_KEY"]
BINANCE_API_SECRET     = os.environ["BINANCE_API_SECRET"]
GOOGLE_CREDENTIALS_B64 = os.environ["GOOGLE_CREDENTIALS_B64"]
GOOGLE_SHEET_ID        = os.environ["GOOGLE_SHEET_ID"]
ADMIN_USER_ID          = int(os.environ.get("ADMIN_USER_ID", "0"))
STOP_LOSS_PCT     = float(os.environ.get("STOP_LOSS_PCT", "0.02"))
TAKE_PROFIT_PCT   = float(os.environ.get("TAKE_PROFIT_PCT", "0.04"))
MAX_POSITION_USDT = float(os.environ.get("MAX_POSITION_USDT", "100"))
MAX_DAILY_LOSS    = float(os.environ.get("MAX_DAILY_LOSS", "50"))

VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def _get_sheet():
    creds_dict = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_B64).decode())
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)

def sheet_append_order(symbol, side, qty, price, status, order_id, strategy="manual"):
    try:
        ws = _get_sheet().worksheet("orders")
        ws.append_row([datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"), symbol, side, float(qty), float(price), status, str(order_id), strategy])
    except Exception as e:
        log.error(f"Sheets order error: {e}")

def sheet_append_position(symbol, entry_price, qty, stop_loss, take_profit, status="OPEN", pnl=0):
    try:
        ws = _get_sheet().worksheet("positions")
        ws.append_row([datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"), symbol, float(entry_price), float(qty), float(stop_loss), float(take_profit), status, float(pnl)])
    except Exception as e:
        log.error(f"Sheets position error: {e}")

def sheet_append_pnl(realized_pnl, total_trades, win_trades):
    try:
        _get_sheet().worksheet("pnl").append_row([datetime.now(VN_TZ).strftime("%Y-%m-%d"), float(realized_pnl), int(total_trades), int(win_trades)])
    except Exception as e:
        log.error(f"Sheets pnl error: {e}")

class RiskManager:
    def __init__(self):
        self.emergency_stop = False
        self.daily_loss = 0.0
        self.loss_streak = 0
        self.daily_trades = 0
        self.daily_win_trades = 0
        self.cooldown_until = None
        self._last_reset = datetime.now(VN_TZ).date()

    def _reset(self):
        today = datetime.now(VN_TZ).date()
        if today != self._last_reset:
            self.daily_loss = 0.0
            self.daily_trades = 0
            self.daily_win_trades = 0
            self._last_reset = today

    def check(self, qty_usdt):
        self._reset()
        if self.emergency_stop: return False, "🛑 Emergency stop đang bật"
        if self.cooldown_until:
            now = datetime.now(VN_TZ)
            if now < self.cooldown_until:
                mins = int((self.cooldown_until - now).total_seconds() / 60)
                return False, f"⏳ Cooldown {mins} phút còn lại"
            self.cooldown_until = None
        if qty_usdt > MAX_POSITION_USDT: return False, f"❌ Vượt max ${MAX_POSITION_USDT} USDT"
        if self.daily_loss >= MAX_DAILY_LOSS: return False, f"❌ Giới hạn lỗ ngày ${self.daily_loss:.2f}/${MAX_DAILY_LOSS}"
        return True, "✅"

    def record_result(self, pnl):
        self._reset()
        self.daily_trades += 1
        if pnl >= 0:
            self.daily_win_trades += 1
            self.loss_streak = 0
        else:
            self.daily_loss += abs(pnl)
            self.loss_streak += 1
            if self.loss_streak >= 3:
                from datetime import timedelta
                self.cooldown_until = datetime.now(VN_TZ) + timedelta(hours=1)

    def status_text(self):
        self._reset()
        return "\n".join([
            f"🛑 Emergency stop: {'BẬT' if self.emergency_stop else 'TẮT'}",
            f"📉 Lỗ hôm nay: ${self.daily_loss:.2f} / ${MAX_DAILY_LOSS}",
            f"📊 Streak thua: {self.loss_streak}",
            f"⏳ Cooldown: {self.cooldown_until.strftime('%H:%M') if self.cooldown_until else 'Không'}",
            f"🔢 Trades: {self.daily_trades} (thắng {self.daily_win_trades})",
        ])

class EMACrossover:
    def __init__(self, fast=9, slow=21):
        self.fast = fast; self.slow = slow
        self.closes = []; self._prev_fast = None; self._prev_slow = None

    def _ema(self, period):
        if len(self.closes) < period: return None
        k = 2 / (period + 1); ema = self.closes[-period]
        for p in self.closes[-period + 1:]: ema = p * k + ema * (1 - k)
        return ema

    def push_candle(self, close):
        self.closes.append(close)
        if len(self.closes) > self.slow * 3: self.closes = self.closes[-(self.slow * 3):]
        cf, cs = self._ema(self.fast), self._ema(self.slow)
        signal = None
        if cf and cs and self._prev_fast and self._prev_slow:
            if self._prev_fast < self._prev_slow and cf > cs: signal = "BUY"
            elif self._prev_fast > self._prev_slow and cf < cs: signal = "SELL"
        self._prev_fast = cf; self._prev_slow = cs
        return signal

intents = nextcord.Intents.default()
bot = commands.Bot(intents=intents)
risk = RiskManager()
ema_strategies = {}
open_positions = {}
binance_client = None

def is_admin(i): return ADMIN_USER_ID == 0 or i.user.id == ADMIN_USER_ID
def symbol_to_usdt(s): s = s.upper(); return s if s.endswith("USDT") else f"{s}USDT"
def round_qty(qty, step): return Decimal(str(qty)).quantize(Decimal(str(step)), rounding=ROUND_DOWN)
async def get_price(symbol): return float((await binance_client.get_symbol_ticker(symbol=symbol.upper()))["price"])
async def get_step_size(symbol):
    info = await binance_client.get_symbol_info(symbol)
    for f in info["filters"]:
        if f["filterType"] == "LOT_SIZE": return float(f["stepSize"])
    return 0.00001
async def notify(msg):
    if DISCORD_CHANNEL_ID == 0: return
    ch = bot.get_channel(DISCORD_CHANNEL_ID)
    if ch: await ch.send(msg)

@bot.slash_command(name="price", description="Xem gia coin hien tai")
async def cmd_price(interaction: nextcord.Interaction, symbol: str = nextcord.SlashOption(description="Vi du BTC ETH")):
    await interaction.response.defer()
    try:
        sym = symbol_to_usdt(symbol)
        price = await get_price(sym)
        embed = nextcord.Embed(title=f"💰 {sym}", color=0x00b894)
        embed.add_field(name="Giá", value=f"${price:,.4f} USDT")
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi: {e}")

@bot.slash_command(name="buy", description="Dat lenh MUA market order")
async def cmd_buy(interaction: nextcord.Interaction,
    symbol: str = nextcord.SlashOption(description="Vi du BTC ETH"),
    usdt_amount: float = nextcord.SlashOption(description="So USDT vi du 50")):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True); return
    await interaction.response.defer()
    sym = symbol_to_usdt(symbol)
    ok, reason = risk.check(usdt_amount)
    if not ok:
        await interaction.followup.send(f"🚫 Risk từ chối:\n{reason}"); return
    try:
        price = await get_price(sym)
        step = await get_step_size(sym)
        qty = round_qty(usdt_amount / price, step)
        if qty <= 0:
            await interaction.followup.send("❌ Qty quá nhỏ."); return
        order = await binance_client.create_order(symbol=sym, side="BUY", type="MARKET", quantity=str(qty))
        fp = float(order.get("fills", [{}])[0].get("price", price))
        sl = fp * (1 - STOP_LOSS_PCT); tp = fp * (1 + TAKE_PROFIT_PCT)
        open_positions[sym] = {"entry_price": fp, "qty": float(qty), "stop_loss": sl, "take_profit": tp, "strategy": "manual", "order_id": order["orderId"]}
        sheet_append_order(sym, "BUY", qty, fp, "FILLED", order["orderId"])
        sheet_append_position(sym, fp, qty, sl, tp)
        embed = nextcord.Embed(title="✅ Lệnh MUA thành công", color=0x00b894)
        embed.add_field(name="Symbol", value=sym, inline=True)
        embed.add_field(name="Qty", value=str(qty), inline=True)
        embed.add_field(name="Giá mua", value=f"${fp:,.4f}", inline=True)
        embed.add_field(name="Stop Loss", value=f"${sl:,.4f} (-{STOP_LOSS_PCT*100:.0f}%)", inline=True)
        embed.add_field(name="Take Profit", value=f"${tp:,.4f} (+{TAKE_PROFIT_PCT*100:.0f}%)", inline=True)
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)
    except BinanceAPIException as e:
        await interaction.followup.send(f"❌ Binance lỗi: {e.message}")
    except Exception as e:
        log.exception("cmd_buy"); await interaction.followup.send(f"❌ Lỗi: {e}")

@bot.slash_command(name="sell", description="Dat lenh BAN market order")
async def cmd_sell(interaction: nextcord.Interaction, symbol: str = nextcord.SlashOption(description="Vi du BTC ETH")):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True); return
    await interaction.response.defer()
    sym = symbol_to_usdt(symbol)
    if sym not in open_positions:
        await interaction.followup.send(f"❌ Không có position {sym}."); return
    pos = open_positions[sym]
    try:
        step = await get_step_size(sym)
        qty = round_qty(pos["qty"], step)
        order = await binance_client.create_order(symbol=sym, side="SELL", type="MARKET", quantity=str(qty))
        sp = float(order.get("fills", [{}])[0].get("price", await get_price(sym)))
        pnl = (sp - pos["entry_price"]) * float(qty)
        pnl_pct = (sp / pos["entry_price"] - 1) * 100
        risk.record_result(pnl)
        sheet_append_order(sym, "SELL", qty, sp, "FILLED", order["orderId"], pos["strategy"])
        sheet_append_position(sym, pos["entry_price"], qty, pos["stop_loss"], pos["take_profit"], "CLOSED", pnl)
        del open_positions[sym]
        icon = "🟢" if pnl >= 0 else "🔴"
        embed = nextcord.Embed(title=f"{icon} Lệnh BÁN thành công", color=0x00b894 if pnl >= 0 else 0xe74c3c)
        embed.add_field(name="Symbol", value=sym, inline=True)
        embed.add_field(name="Giá bán", value=f"${sp:,.4f}", inline=True)
        embed.add_field(name="PnL", value=f"${pnl:+.2f} ({pnl_pct:+.2f}%)", inline=True)
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)
    except BinanceAPIException as e:
        await interaction.followup.send(f"❌ Binance lỗi: {e.message}")
    except Exception as e:
        log.exception("cmd_sell"); await interaction.followup.send(f"❌ Lỗi: {e}")

@bot.slash_command(name="positions", description="Xem cac position dang mo")
async def cmd_positions(interaction: nextcord.Interaction):
    await interaction.response.defer()
    if not open_positions:
        await interaction.followup.send("📭 Không có position nào đang mở."); return
    embed = nextcord.Embed(title="📊 Open Positions", color=0x6c5ce7)
    embed.timestamp = datetime.now(timezone.utc)
    for sym, pos in open_positions.items():
        try:
            cur = await get_price(sym)
            pnl = (cur - pos["entry_price"]) * pos["qty"]
            pnl_pct = (cur / pos["entry_price"] - 1) * 100
            icon = "🟢" if pnl >= 0 else "🔴"
            embed.add_field(name=sym, value=f"Vào: ${pos['entry_price']:,.4f}\nHiện: ${cur:,.4f}\nPnL: {icon} ${pnl:+.2f} ({pnl_pct:+.2f}%)\nSL: ${pos['stop_loss']:,.4f} | TP: ${pos['take_profit']:,.4f}", inline=False)
        except: embed.add_field(name=sym, value="(lỗi lấy giá)", inline=False)
    await interaction.followup.send(embed=embed)

@bot.slash_command(name="portfolio", description="Xem so du Binance")
async def cmd_portfolio(interaction: nextcord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    try:
        balances = [b for b in (await binance_client.get_account())["balances"] if float(b["free"]) > 0 or float(b["locked"]) > 0]
        embed = nextcord.Embed(title="💼 Portfolio Binance", color=0x6c5ce7)
        for b in balances[:20]:
            total = float(b["free"]) + float(b["locked"])
            embed.add_field(name=b["asset"], value=f"{total:.6f}" + (f" (🔒{float(b['locked']):.6f})" if float(b["locked"]) > 0 else ""), inline=True)
        if len(balances) > 20: embed.set_footer(text=f"...và {len(balances)-20} asset khác")
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi: {e}")

@bot.slash_command(name="risk", description="Xem trang thai risk manager")
async def cmd_risk(interaction: nextcord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True); return
    await interaction.response.send_message(f"🛡️ **Risk Manager**\n{risk.status_text()}", ephemeral=True)

@bot.slash_command(name="stop", description="Tat khan cap dung moi giao dich")
async def cmd_stop(interaction: nextcord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True); return
    risk.emergency_stop = True
    await interaction.response.send_message("🛑 **Emergency stop đã bật.**", ephemeral=True)
    await notify("🛑 **EMERGENCY STOP** kích hoạt qua Discord!")

@bot.slash_command(name="resume", description="Mo lai giao dich sau emergency stop")
async def cmd_resume(interaction: nextcord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True); return
    risk.emergency_stop = False
    await interaction.response.send_message("✅ Emergency stop đã tắt.", ephemeral=True)

@bot.slash_command(name="strategy", description="Bat tat auto trading")
async def cmd_strategy(interaction: nextcord.Interaction,
    symbol: str = nextcord.SlashOption(description="Vi du BTC ETH"),
    action: str = nextcord.SlashOption(description="bat hoac tat", choices=["bat", "tat"]),
    interval: str = nextcord.SlashOption(description="1m 5m 15m 1h 4h mac dinh 1h", required=False, default="1h")):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True); return
    sym = symbol_to_usdt(symbol)
    if action == "bat":
        ema_strategies[sym] = {"strategy": EMACrossover(), "interval": interval}
        await interaction.response.send_message(f"✅ Auto trading **bật** cho {sym} (EMA 9/21, khung {interval})")
    else:
        ema_strategies.pop(sym, None)
        await interaction.response.send_message(f"⏹️ Auto trading **tắt** cho {sym}.")

@bot.slash_command(name="strategies", description="Danh sach strategy dang chay")
async def cmd_strategies(interaction: nextcord.Interaction):
    if not ema_strategies:
        await interaction.response.send_message("📭 Không có strategy nào.", ephemeral=True); return
    lines = [f"• **{sym}** — EMA 9/21 — {v['interval']}" for sym, v in ema_strategies.items()]
    await interaction.response.send_message("⚙️ **Strategies:**\n" + "\n".join(lines), ephemeral=True)

@tasks.loop(minutes=1)
async def auto_trade_loop():
    if risk.emergency_stop: return
    for sym, pos in list(open_positions.items()):
        try:
            price = await get_price(sym)
            if price <= pos["stop_loss"] or price >= pos["take_profit"]:
                label = "STOP LOSS" if price <= pos["stop_loss"] else "TAKE PROFIT"
                step = await get_step_size(sym)
                qty = round_qty(pos["qty"], step)
                order = await binance_client.create_order(symbol=sym, side="SELL", type="MARKET", quantity=str(qty))
                sp = float(order.get("fills", [{}])[0].get("price", price))
                pnl = (sp - pos["entry_price"]) * float(qty)
                risk.record_result(pnl)
                sheet_append_order(sym, "SELL", qty, sp, "FILLED", order["orderId"], pos.get("strategy", "auto"))
                sheet_append_position(sym, pos["entry_price"], qty, pos["stop_loss"], pos["take_profit"], "CLOSED", pnl)
                del open_positions[sym]
                await notify(f"{'🟢' if pnl>=0 else '🔴'} **{label}** {sym}\nGiá: ${sp:,.4f} | PnL: ${pnl:+.2f}")
        except Exception as e:
            log.error(f"SL/TP {sym}: {e}")
    for sym, cfg in list(ema_strategies.items()):
        try:
            candles = await binance_client.get_klines(symbol=sym, interval=cfg["interval"], limit=30)
            signal = None
            for c in candles[:-1]: signal = cfg["strategy"].push_candle(float(c[4]))
            if signal == "BUY" and sym not in open_positions:
                price = await get_price(sym)
                ok, reason = risk.check(MAX_POSITION_USDT * 0.5)
                if ok:
                    step = await get_step_size(sym)
                    qty = round_qty((MAX_POSITION_USDT * 0.5) / price, step)
                    order = await binance_client.create_order(symbol=sym, side="BUY", type="MARKET", quantity=str(qty))
                    fp = float(order.get("fills", [{}])[0].get("price", price))
                    sl = fp * (1 - STOP_LOSS_PCT); tp = fp * (1 + TAKE_PROFIT_PCT)
                    open_positions[sym] = {"entry_price": fp, "qty": float(qty), "stop_loss": sl, "take_profit": tp, "strategy": "ema_crossover", "order_id": order["orderId"]}
                    sheet_append_order(sym, "BUY", qty, fp, "FILLED", order["orderId"], "ema_crossover")
                    sheet_append_position(sym, fp, qty, sl, tp)
                    await notify(f"🤖 **AUTO BUY** {sym}\nEMA 9/21 golden cross\nGiá: ${fp:,.4f} | SL: ${sl:,.4f} | TP: ${tp:,.4f}")
            elif signal == "SELL" and sym in open_positions:
                pos = open_positions[sym]
                step = await get_step_size(sym)
                qty = round_qty(pos["qty"], step)
                order = await binance_client.create_order(symbol=sym, side="SELL", type="MARKET", quantity=str(qty))
                sp = float(order.get("fills", [{}])[0].get("price", await get_price(sym)))
                pnl = (sp - pos["entry_price"]) * float(qty)
                risk.record_result(pnl)
                sheet_append_order(sym, "SELL", qty, sp, "FILLED", order["orderId"], "ema_crossover")
                sheet_append_position(sym, pos["entry_price"], qty, pos["stop_loss"], pos["take_profit"], "CLOSED", pnl)
                del open_positions[sym]
                await notify(f"🤖 **AUTO SELL** {sym}\nEMA 9/21 death cross\nGiá: ${sp:,.4f} | PnL: {'🟢' if pnl>=0 else '🔴'} ${pnl:+.2f}")
        except Exception as e:
            log.error(f"Strategy {sym}: {e}")

@tasks.loop(hours=24)
async def daily_report():
    winrate = (risk.daily_win_trades / risk.daily_trades * 100) if risk.daily_trades > 0 else 0
    sheet_append_pnl(risk.daily_loss * -1, risk.daily_trades, risk.daily_win_trades)
    await notify(f"📋 **Báo cáo {datetime.now(VN_TZ).strftime('%d/%m/%Y')}**\n📊 Trades: {risk.daily_trades} (thắng {risk.daily_win_trades}, winrate {winrate:.0f}%)\n💰 PnL: ${risk.daily_loss*-1:+.2f} USDT")

@bot.event
async def on_ready():
    global binance_client
    log.info(f"Bot ready: {bot.user}")
    binance_client = await AsyncClient.create(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
    log.info("Binance connected")
    if not auto_trade_loop.is_running(): auto_trade_loop.start()
    if not daily_report.is_running():
        from datetime import timedelta
        now = datetime.now(VN_TZ)
        target = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if now >= target: target += timedelta(days=1)
        bot.loop.call_later((target - now).total_seconds(), daily_report.start)
    await notify(f"🚀 **Trading bot online** — {datetime.now(VN_TZ).strftime('%d/%m/%Y %H:%M')} VN")

bot.run(DISCORD_TOKEN)

"""
Binance Trading Bot — Discord
Stack: discord.py + python-binance + gspread (Google Sheets)
Deploy: Railway (single worker, no Docker, no Redis, no Postgres)

Google Sheets schema:
  Tab "orders":    Date | Symbol | Side | Qty | Price | Status | OrderID | Strategy
  Tab "positions": Date | Symbol | EntryPrice | Qty | StopLoss | TakeProfit | Status | PnL
  Tab "pnl":       Date | RealizedPnL | TotalTrades | WinTrades
"""

import os
import json
import base64
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import discord
from discord.ext import commands, tasks
from discord import app_commands
from binance import AsyncClient
from binance.exceptions import BinanceAPIException
import gspread
from google.oauth2.service_account import Credentials
import pytz

# ─────────────────────────────────────────────────────────────
# CONFIG — đọc từ Railway environment variables
# ─────────────────────────────────────────────────────────────

DISCORD_TOKEN         = os.environ["DISCORD_TOKEN"]
DISCORD_CHANNEL_ID    = int(os.environ.get("DISCORD_CHANNEL_ID", "0"))
BINANCE_API_KEY       = os.environ["BINANCE_API_KEY"]
BINANCE_API_SECRET    = os.environ["BINANCE_API_SECRET"]
GOOGLE_CREDENTIALS_B64 = os.environ["GOOGLE_CREDENTIALS_B64"]
GOOGLE_SHEET_ID       = os.environ["GOOGLE_SHEET_ID"]
ADMIN_USER_ID         = int(os.environ.get("ADMIN_USER_ID", "0"))  # your Discord user ID

# Risk settings — đổi trực tiếp ở đây hoặc qua env
STOP_LOSS_PCT     = float(os.environ.get("STOP_LOSS_PCT", "0.02"))      # 2%
TAKE_PROFIT_PCT   = float(os.environ.get("TAKE_PROFIT_PCT", "0.04"))    # 4%
MAX_POSITION_USDT = float(os.environ.get("MAX_POSITION_USDT", "100"))   # $100
MAX_DAILY_LOSS    = float(os.environ.get("MAX_DAILY_LOSS", "50"))        # $50/ngày

VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# GOOGLE SHEETS HELPER
# ─────────────────────────────────────────────────────────────

def _get_sheet():
    """Mở Google Sheets. Gọi mỗi lần write để tránh token expire."""
    creds_json = base64.b64decode(GOOGLE_CREDENTIALS_B64).decode()
    creds_dict = json.loads(creds_json)
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc     = gspread.authorize(creds)
    return gc.open_by_key(GOOGLE_SHEET_ID)


def sheet_append_order(symbol, side, qty, price, status, order_id, strategy="manual"):
    """Ghi 1 dòng vào tab 'orders'."""
    try:
        ws = _get_sheet().worksheet("orders")
        now = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([now, symbol, side, float(qty), float(price), status, str(order_id), strategy])
        log.info(f"Sheets: order logged {side} {symbol}")
    except Exception as e:
        log.error(f"Sheets write error (order): {e}")


def sheet_append_position(symbol, entry_price, qty, stop_loss, take_profit, status="OPEN", pnl=0):
    """Ghi 1 dòng vào tab 'positions'."""
    try:
        ws = _get_sheet().worksheet("positions")
        now = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([now, symbol, float(entry_price), float(qty),
                       float(stop_loss), float(take_profit), status, float(pnl)])
        log.info(f"Sheets: position logged {symbol} {status}")
    except Exception as e:
        log.error(f"Sheets write error (position): {e}")


def sheet_append_pnl(realized_pnl, total_trades, win_trades):
    """Ghi daily PnL snapshot vào tab 'pnl'."""
    try:
        ws = _get_sheet().worksheet("pnl")
        today = datetime.now(VN_TZ).strftime("%Y-%m-%d")
        ws.append_row([today, float(realized_pnl), int(total_trades), int(win_trades)])
    except Exception as e:
        log.error(f"Sheets write error (pnl): {e}")


# ─────────────────────────────────────────────────────────────
# RISK MANAGER (in-memory, reset khi bot restart)
# ─────────────────────────────────────────────────────────────

class RiskManager:
    def __init__(self):
        self.emergency_stop   = False
        self.daily_loss       = 0.0
        self.loss_streak      = 0
        self.daily_trades     = 0
        self.daily_win_trades = 0
        self.cooldown_until   = None   # datetime | None
        self._last_reset_date = datetime.now(VN_TZ).date()

    def _maybe_reset(self):
        today = datetime.now(VN_TZ).date()
        if today != self._last_reset_date:
            self.daily_loss       = 0.0
            self.daily_trades     = 0
            self.daily_win_trades = 0
            self._last_reset_date = today
            log.info("Risk: daily counters reset")

    def check(self, qty_usdt: float) -> tuple[bool, str]:
        """Returns (approved: bool, reason: str)."""
        self._maybe_reset()

        if self.emergency_stop:
            return False, "🛑 Emergency stop đang bật"

        if self.cooldown_until:
            now = datetime.now(VN_TZ)
            if now < self.cooldown_until:
                mins = int((self.cooldown_until - now).total_seconds() / 60)
                return False, f"⏳ Cooldown {mins} phút còn lại (thua {self.loss_streak} lần liên tiếp)"
            self.cooldown_until = None

        if qty_usdt > MAX_POSITION_USDT:
            return False, f"❌ Vượt max position ${MAX_POSITION_USDT} USDT"

        if self.daily_loss >= MAX_DAILY_LOSS:
            return False, f"❌ Đạt giới hạn lỗ ngày ${self.daily_loss:.2f}/${MAX_DAILY_LOSS}"

        return True, "✅"

    def record_result(self, pnl: float):
        self._maybe_reset()
        self.daily_trades += 1
        if pnl >= 0:
            self.daily_win_trades += 1
            self.loss_streak = 0
        else:
            self.daily_loss  += abs(pnl)
            self.loss_streak += 1
            if self.loss_streak >= 3:
                from datetime import timedelta
                self.cooldown_until = datetime.now(VN_TZ) + timedelta(hours=1)
                log.warning(f"Risk: cooldown 1h sau {self.loss_streak} thua liên tiếp")

    def status_text(self) -> str:
        self._maybe_reset()
        lines = [
            f"🛑 Emergency stop: {'BẬT' if self.emergency_stop else 'TẮT'}",
            f"📉 Lỗ hôm nay: ${self.daily_loss:.2f} / ${MAX_DAILY_LOSS}",
            f"📊 Streak thua: {self.loss_streak}",
            f"⏳ Cooldown: {self.cooldown_until.strftime('%H:%M') if self.cooldown_until else 'Không'}",
            f"🔢 Trades hôm nay: {self.daily_trades} (thắng {self.daily_win_trades})",
        ]
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# EMA CROSSOVER STRATEGY (giữ state trong memory)
# ─────────────────────────────────────────────────────────────

class EMACrossover:
    """
    Signal: BUY khi EMA9 vượt lên trên EMA21 (golden cross)
            SELL khi EMA9 xuống dưới EMA21 (death cross)
    """
    def __init__(self, fast=9, slow=21):
        self.fast = fast
        self.slow = slow
        self.closes: list[float] = []
        self._prev_fast = None
        self._prev_slow = None

    def _ema(self, period: int) -> float | None:
        if len(self.closes) < period:
            return None
        k   = 2 / (period + 1)
        ema = self.closes[-period]
        for p in self.closes[-period + 1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def push_candle(self, close: float) -> str | None:
        """
        Nhận giá close mới. Trả về "BUY" | "SELL" | None.
        Chỉ gọi với closed candles.
        """
        self.closes.append(close)
        if len(self.closes) > self.slow * 3:
            self.closes = self.closes[-(self.slow * 3):]

        cur_fast = self._ema(self.fast)
        cur_slow = self._ema(self.slow)

        signal = None
        if cur_fast and cur_slow and self._prev_fast and self._prev_slow:
            if self._prev_fast < self._prev_slow and cur_fast > cur_slow:
                signal = "BUY"
            elif self._prev_fast > self._prev_slow and cur_fast < cur_slow:
                signal = "SELL"

        self._prev_fast = cur_fast
        self._prev_slow = cur_slow
        return signal


# ─────────────────────────────────────────────────────────────
# BOT
# ─────────────────────────────────────────────────────────────

intents = discord.Intents.default()
bot     = commands.Bot(command_prefix="!", intents=intents)
tree    = bot.tree

# Singletons
risk = RiskManager()
ema_strategies: dict[str, EMACrossover] = {}  # symbol → strategy
binance_client: AsyncClient | None = None

# Tracking open positions in memory
# { symbol: { entry_price, qty, stop_loss, take_profit, strategy } }
open_positions: dict[str, dict] = {}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def is_admin(interaction: discord.Interaction) -> bool:
    if ADMIN_USER_ID == 0:
        return True  # dev mode — no restriction
    return interaction.user.id == ADMIN_USER_ID


async def get_price(symbol: str) -> float:
    ticker = await binance_client.get_symbol_ticker(symbol=symbol.upper())
    return float(ticker["price"])


async def notify(msg: str):
    """Gửi message vào Discord channel."""
    if DISCORD_CHANNEL_ID == 0:
        return
    ch = bot.get_channel(DISCORD_CHANNEL_ID)
    if ch:
        await ch.send(msg)


def symbol_to_usdt(symbol: str) -> str:
    """BTC → BTCUSDT, BTCUSDT → BTCUSDT"""
    s = symbol.upper()
    return s if s.endswith("USDT") else f"{s}USDT"


def round_qty(qty: float, step: float) -> Decimal:
    """Làm tròn qty theo stepSize của Binance."""
    step_dec = Decimal(str(step))
    return Decimal(str(qty)).quantize(step_dec, rounding=ROUND_DOWN)


async def get_step_size(symbol: str) -> float:
    """Lấy stepSize (bước qty nhỏ nhất) từ Binance exchange info."""
    info = await binance_client.get_symbol_info(symbol)
    for f in info["filters"]:
        if f["filterType"] == "LOT_SIZE":
            return float(f["stepSize"])
    return 0.00001


# ─────────────────────────────────────────────────────────────
# SLASH COMMANDS
# ─────────────────────────────────────────────────────────────

@tree.command(name="price", description="Xem gia coin hien tai")
@app_commands.describe(symbol="Vi du: BTC, ETH, BTCUSDT")
async def cmd_price(interaction: discord.Interaction, symbol: str):
    await interaction.response.defer()
    try:
        sym   = symbol_to_usdt(symbol)
        price = await get_price(sym)
        embed = discord.Embed(title=f"💰 {sym}", color=0x00b894)
        embed.add_field(name="Giá", value=f"${price:,.4f} USDT")
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi: {e}")


@tree.command(name="buy", description="Dat lenh MUA (market order)")
@app_commands.describe(
    symbol="Vi du: BTC, ETH",
    usdt_amount="So USDT muon mua (vi du: 50)"
)
async def cmd_buy(interaction: discord.Interaction, symbol: str, usdt_amount: float):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True)
        return

    await interaction.response.defer()
    sym = symbol_to_usdt(symbol)

    # Risk check
    ok, reason = risk.check(usdt_amount)
    if not ok:
        await interaction.followup.send(f"🚫 Risk từ chối lệnh:\n{reason}")
        return

    try:
        price    = await get_price(sym)
        step     = await get_step_size(sym)
        raw_qty  = usdt_amount / price
        qty      = round_qty(raw_qty, step)

        if qty <= 0:
            await interaction.followup.send("❌ Qty quá nhỏ sau làm tròn.")
            return

        order = await binance_client.create_order(
            symbol=sym,
            side="BUY",
            type="MARKET",
            quantity=str(qty),
        )

        filled_price = float(order.get("fills", [{}])[0].get("price", price))
        stop_loss    = filled_price * (1 - STOP_LOSS_PCT)
        take_profit  = filled_price * (1 + TAKE_PROFIT_PCT)

        # Lưu vào memory
        open_positions[sym] = {
            "entry_price":  filled_price,
            "qty":          float(qty),
            "stop_loss":    stop_loss,
            "take_profit":  take_profit,
            "strategy":     "manual",
            "order_id":     order["orderId"],
        }

        # Lưu vào Google Sheets
        sheet_append_order(sym, "BUY", qty, filled_price, "FILLED", order["orderId"])
        sheet_append_position(sym, filled_price, qty, stop_loss, take_profit)

        embed = discord.Embed(title="✅ Lệnh MUA thành công", color=0x00b894)
        embed.add_field(name="Symbol",      value=sym,                          inline=True)
        embed.add_field(name="Qty",         value=str(qty),                     inline=True)
        embed.add_field(name="Giá mua",     value=f"${filled_price:,.4f}",      inline=True)
        embed.add_field(name="Stop Loss",   value=f"${stop_loss:,.4f} (-{STOP_LOSS_PCT*100:.0f}%)",   inline=True)
        embed.add_field(name="Take Profit", value=f"${take_profit:,.4f} (+{TAKE_PROFIT_PCT*100:.0f}%)", inline=True)
        embed.add_field(name="Order ID",    value=str(order["orderId"]),         inline=True)
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)

    except BinanceAPIException as e:
        await interaction.followup.send(f"❌ Binance lỗi: {e.message}")
    except Exception as e:
        log.exception("cmd_buy error")
        await interaction.followup.send(f"❌ Lỗi: {e}")


@tree.command(name="sell", description="Dat lenh BAN (market order)")
@app_commands.describe(symbol="Vi du: BTC, ETH")
async def cmd_sell(interaction: discord.Interaction, symbol: str):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True)
        return

    await interaction.response.defer()
    sym = symbol_to_usdt(symbol)

    if sym not in open_positions:
        await interaction.followup.send(f"❌ Không có position {sym} đang mở.")
        return

    pos = open_positions[sym]

    try:
        step     = await get_step_size(sym)
        qty      = round_qty(pos["qty"], step)
        order    = await binance_client.create_order(
            symbol=sym,
            side="SELL",
            type="MARKET",
            quantity=str(qty),
        )

        sell_price = float(order.get("fills", [{}])[0].get("price", await get_price(sym)))
        pnl        = (sell_price - pos["entry_price"]) * float(qty)
        pnl_pct    = (sell_price / pos["entry_price"] - 1) * 100

        # Cập nhật risk manager
        risk.record_result(pnl)

        # Lưu Sheets
        sheet_append_order(sym, "SELL", qty, sell_price, "FILLED", order["orderId"], pos["strategy"])
        sheet_append_position(sym, pos["entry_price"], qty, pos["stop_loss"], pos["take_profit"], "CLOSED", pnl)

        del open_positions[sym]

        color = 0x00b894 if pnl >= 0 else 0xe74c3c
        icon  = "🟢" if pnl >= 0 else "🔴"
        embed = discord.Embed(title=f"{icon} Lệnh BÁN thành công", color=color)
        embed.add_field(name="Symbol",    value=sym,                     inline=True)
        embed.add_field(name="Giá bán",  value=f"${sell_price:,.4f}",   inline=True)
        embed.add_field(name="PnL",      value=f"${pnl:+.2f} ({pnl_pct:+.2f}%)", inline=True)
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)

    except BinanceAPIException as e:
        await interaction.followup.send(f"❌ Binance lỗi: {e.message}")
    except Exception as e:
        log.exception("cmd_sell error")
        await interaction.followup.send(f"❌ Lỗi: {e}")


@tree.command(name="positions", description="Xem cac position dang mo")
async def cmd_positions(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if not open_positions:
        await interaction.followup.send("📭 Không có position nào đang mở.")
        return

    embed = discord.Embed(title="📊 Open Positions", color=0x6c5ce7)
    embed.timestamp = datetime.now(timezone.utc)

    for sym, pos in open_positions.items():
        try:
            current = await get_price(sym)
            pnl     = (current - pos["entry_price"]) * pos["qty"]
            pnl_pct = (current / pos["entry_price"] - 1) * 100
            icon    = "🟢" if pnl >= 0 else "🔴"
            embed.add_field(
                name=sym,
                value=(
                    f"Vào: ${pos['entry_price']:,.4f}\n"
                    f"Hiện: ${current:,.4f}\n"
                    f"PnL: {icon} ${pnl:+.2f} ({pnl_pct:+.2f}%)\n"
                    f"SL: ${pos['stop_loss']:,.4f} | TP: ${pos['take_profit']:,.4f}"
                ),
                inline=False,
            )
        except Exception:
            embed.add_field(name=sym, value="(lỗi lấy giá)", inline=False)

    await interaction.followup.send(embed=embed)


@tree.command(name="portfolio", description="Xem so du Binance")
async def cmd_portfolio(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        account  = await binance_client.get_account()
        balances = [
            b for b in account["balances"]
            if float(b["free"]) > 0 or float(b["locked"]) > 0
        ]
        embed = discord.Embed(title="💼 Portfolio Binance", color=0x6c5ce7)
        for b in balances[:20]:
            total  = float(b["free"]) + float(b["locked"])
            locked = float(b["locked"])
            val    = f"{total:.6f}"
            if locked > 0:
                val += f" (🔒 {locked:.6f})"
            embed.add_field(name=b["asset"], value=val, inline=True)
        if len(balances) > 20:
            embed.set_footer(text=f"...và {len(balances)-20} asset khác")
        embed.timestamp = datetime.now(timezone.utc)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi: {e}")


@tree.command(name="risk", description="Xem trang thai risk manager")
async def cmd_risk(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"🛡️ **Risk Manager**\n{risk.status_text()}", ephemeral=True
    )


@tree.command(name="stop", description="Tat khan cap - dung moi giao dich")
async def cmd_stop(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True)
        return
    risk.emergency_stop = True
    await interaction.response.send_message("🛑 **Emergency stop đã bật.** Không có lệnh nào được đặt.", ephemeral=True)
    await notify("🛑 **EMERGENCY STOP** được kích hoạt qua Discord!")


@tree.command(name="resume", description="Mo lai giao dich sau emergency stop")
async def cmd_resume(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True)
        return
    risk.emergency_stop = False
    await interaction.response.send_message("✅ Emergency stop đã tắt. Trading tiếp tục.", ephemeral=True)


@tree.command(name="strategy", description="Bat/tat auto trading cho 1 symbol")
@app_commands.describe(
    symbol="Vi du: BTC, ETH",
    action="bat hoac tat",
    interval="Khung gio: 1m 5m 15m 1h 4h (mac dinh 1h)"
)
@app_commands.choices(action=[
    app_commands.Choice(name="bat", value="on"),
    app_commands.Choice(name="tat", value="off"),
])
async def cmd_strategy(
    interaction: discord.Interaction,
    symbol: str,
    action: str,
    interval: str = "1h"
):
    if not is_admin(interaction):
        await interaction.response.send_message("⛔ Không có quyền.", ephemeral=True)
        return

    sym = symbol_to_usdt(symbol)

    if action == "on":
        ema_strategies[sym] = {"strategy": EMACrossover(), "interval": interval}
        await interaction.response.send_message(
            f"✅ Auto trading **bật** cho {sym} (EMA 9/21, khung {interval})\n"
            f"Bot sẽ tự BUY/SELL khi có tín hiệu EMA crossover."
        )
        log.info(f"Strategy ON: {sym} {interval}")
    else:
        if sym in ema_strategies:
            del ema_strategies[sym]
        await interaction.response.send_message(f"⏹️ Auto trading **tắt** cho {sym}.")
        log.info(f"Strategy OFF: {sym}")


@tree.command(name="strategies", description="Danh sach strategy dang chay")
async def cmd_strategies(interaction: discord.Interaction):
    if not ema_strategies:
        await interaction.response.send_message("📭 Không có strategy nào đang chạy.", ephemeral=True)
        return
    lines = [f"• **{sym}** — EMA 9/21 — khung {v['interval']}" for sym, v in ema_strategies.items()]
    await interaction.response.send_message("⚙️ **Strategies đang chạy:**\n" + "\n".join(lines), ephemeral=True)


# ─────────────────────────────────────────────────────────────
# AUTO TRADING LOOP (chạy mỗi phút, check closed candles)
# ─────────────────────────────────────────────────────────────

@tasks.loop(minutes=1)
async def auto_trade_loop():
    """
    Chạy mỗi phút:
    1. Check stop-loss / take-profit cho open positions
    2. Fetch candles cho các symbol có strategy bật
    3. Generate signal → đặt lệnh nếu có
    """
    if risk.emergency_stop:
        return

    # ── Check SL/TP ──────────────────────────────────────────
    for sym, pos in list(open_positions.items()):
        try:
            price = await get_price(sym)

            hit_sl = price <= pos["stop_loss"]
            hit_tp = price >= pos["take_profit"]

            if hit_sl or hit_tp:
                label  = "STOP LOSS" if hit_sl else "TAKE PROFIT"
                step   = await get_step_size(sym)
                qty    = round_qty(pos["qty"], step)
                order  = await binance_client.create_order(
                    symbol=sym, side="SELL", type="MARKET", quantity=str(qty)
                )
                sell_price = float(order.get("fills", [{}])[0].get("price", price))
                pnl        = (sell_price - pos["entry_price"]) * float(qty)

                risk.record_result(pnl)
                sheet_append_order(sym, "SELL", qty, sell_price, "FILLED", order["orderId"], pos.get("strategy", "auto"))
                sheet_append_position(sym, pos["entry_price"], qty, pos["stop_loss"], pos["take_profit"], "CLOSED", pnl)
                del open_positions[sym]

                icon = "🟢" if pnl >= 0 else "🔴"
                await notify(
                    f"{icon} **{label}** hit cho {sym}\n"
                    f"Giá: ${sell_price:,.4f} | PnL: ${pnl:+.2f}"
                )
        except Exception as e:
            log.error(f"SL/TP check error {sym}: {e}")

    # ── Check strategy signals ────────────────────────────────
    for sym, cfg in list(ema_strategies.items()):
        try:
            interval = cfg["interval"]
            strat    = cfg["strategy"]

            # Lấy 30 candles gần nhất, lấy candle [-2] vì [-1] chưa đóng
            candles  = await binance_client.get_klines(symbol=sym, interval=interval, limit=30)
            closed   = candles[:-1]  # bỏ candle đang hình thành

            # Push tất cả vào strategy (nó tự dedup qua buffer)
            signal = None
            for c in closed:
                signal = strat.push_candle(float(c[4]))  # close price

            if signal == "BUY" and sym not in open_positions:
                price     = await get_price(sym)
                ok, reason = risk.check(MAX_POSITION_USDT * 0.5)  # dùng 50% max
                if ok:
                    step  = await get_step_size(sym)
                    qty   = round_qty((MAX_POSITION_USDT * 0.5) / price, step)
                    order = await binance_client.create_order(
                        symbol=sym, side="BUY", type="MARKET", quantity=str(qty)
                    )
                    fp  = float(order.get("fills", [{}])[0].get("price", price))
                    sl  = fp * (1 - STOP_LOSS_PCT)
                    tp  = fp * (1 + TAKE_PROFIT_PCT)
                    open_positions[sym] = {
                        "entry_price": fp, "qty": float(qty),
                        "stop_loss": sl, "take_profit": tp, "strategy": "ema_crossover",
                        "order_id": order["orderId"],
                    }
                    sheet_append_order(sym, "BUY", qty, fp, "FILLED", order["orderId"], "ema_crossover")
                    sheet_append_position(sym, fp, qty, sl, tp)
                    await notify(
                        f"🤖 **AUTO BUY** {sym}\n"
                        f"Signal: EMA 9/21 golden cross ({interval})\n"
                        f"Giá: ${fp:,.4f} | SL: ${sl:,.4f} | TP: ${tp:,.4f}"
                    )
                else:
                    log.info(f"Auto BUY {sym} bị chặn: {reason}")

            elif signal == "SELL" and sym in open_positions:
                pos   = open_positions[sym]
                step  = await get_step_size(sym)
                qty   = round_qty(pos["qty"], step)
                order = await binance_client.create_order(
                    symbol=sym, side="SELL", type="MARKET", quantity=str(qty)
                )
                sp  = float(order.get("fills", [{}])[0].get("price", await get_price(sym)))
                pnl = (sp - pos["entry_price"]) * float(qty)
                risk.record_result(pnl)
                sheet_append_order(sym, "SELL", qty, sp, "FILLED", order["orderId"], "ema_crossover")
                sheet_append_position(sym, pos["entry_price"], qty, pos["stop_loss"], pos["take_profit"], "CLOSED", pnl)
                del open_positions[sym]
                icon = "🟢" if pnl >= 0 else "🔴"
                await notify(
                    f"🤖 **AUTO SELL** {sym}\n"
                    f"Signal: EMA 9/21 death cross ({interval})\n"
                    f"Giá: ${sp:,.4f} | PnL: {icon} ${pnl:+.2f}"
                )

        except Exception as e:
            log.error(f"Strategy loop error {sym}: {e}")


# ─────────────────────────────────────────────────────────────
# DAILY PNL REPORT (7:00 sáng VN mỗi ngày)
# ─────────────────────────────────────────────────────────────

@tasks.loop(hours=24)
async def daily_report():
    now_vn = datetime.now(VN_TZ)
    sheet_append_pnl(risk.daily_loss * -1, risk.daily_trades, risk.daily_win_trades)
    winrate = (risk.daily_win_trades / risk.daily_trades * 100) if risk.daily_trades > 0 else 0
    await notify(
        f"📋 **Báo cáo ngày {now_vn.strftime('%d/%m/%Y')}**\n"
        f"📊 Trades: {risk.daily_trades} (thắng {risk.daily_win_trades}, winrate {winrate:.0f}%)\n"
        f"💰 PnL: ${risk.daily_loss * -1:+.2f} USDT"
    )


# ─────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    global binance_client

    log.info(f"Discord bot ready: {bot.user}")

    # Kết nối Binance
    binance_client = await AsyncClient.create(
        api_key=BINANCE_API_KEY,
        api_secret=BINANCE_API_SECRET,
    )
    log.info("Binance connected (mainnet)")

    # Sync slash commands
    await tree.sync()
    log.info("Slash commands synced")

    # Khởi động loops
    if not auto_trade_loop.is_running():
        auto_trade_loop.start()

    if not daily_report.is_running():
        # Tính thời gian đến 7:00 sáng VN
        now   = datetime.now(VN_TZ)
        target = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if now >= target:
            from datetime import timedelta
            target += timedelta(days=1)
        delay = (target - now).total_seconds()
        bot.loop.call_later(delay, daily_report.start)

    await notify(f"🚀 **Trading bot online** — {datetime.now(VN_TZ).strftime('%d/%m/%Y %H:%M')} VN")
    log.info("All systems started")


@bot.event
async def on_disconnect():
    log.warning("Discord disconnected")


bot.run(DISCORD_TOKEN)

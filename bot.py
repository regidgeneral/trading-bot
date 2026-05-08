"""
Binance Trading Bot — Discord
Stack: discord.py + audioop-lts + python-binance + gspread
"""
import os, json, base64, logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import discord
from discord import app_commands
from discord.ext import commands, tasks
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
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)

def sheet_append_order(symbol, side, qty, price, status, order_id, strategy="manual"):
    try:
        _get_sheet().worksheet("orders").append_row([datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"), symbol, side, float(qty), float(price), status, str(order_id), strategy])
    except Exception as e: log.error(f"Sheets order: {e}")

def sheet_append_position(symbol, entry_price, qty, sl, tp, status="OPEN", pnl=0):
    try:
        _get_sheet().worksheet("positions").append_row([datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"), symbol, float(entry_price), float(qty), float(sl), float(tp), status, float(pnl)])
    except Exception as e: log.error(f"Sheets position: {e}")

def sheet_append_pnl(pnl, total, wins):
    try:
        _get_sheet().worksheet("pnl").append_row([datetime.now(VN_TZ).strftime("%Y-%m-%d"), float(pnl), int(total), int(wins)])
    except Exception as e: log.error(f"Sheets pnl: {e}")

class RiskManager:
    def __init__(self):
        self.emergency_stop=False; self.daily_loss=0.0; self.loss_streak=0
        self.daily_trades=0; self.daily_win_trades=0; self.cooldown_until=None
        self._last_reset=datetime.now(VN_TZ).date()
    def _reset(self):
        today=datetime.now(VN_TZ).date()
        if today!=self._last_reset:
            self.daily_loss=0.0; self.daily_trades=0; self.daily_win_trades=0; self._last_reset=today
    def check(self, qty_usdt):
        self._reset()
        if self.emergency_stop: return False,"🛑 Emergency stop đang bật"
        if self.cooldown_until:
            now=datetime.now(VN_TZ)
            if now<self.cooldown_until: return False,f"⏳ Cooldown {int((self.cooldown_until-now).total_seconds()/60)} phút"
            self.cooldown_until=None
        if qty_usdt>MAX_POSITION_USDT: return False,f"❌ Vượt max ${MAX_POSITION_USDT}"
        if self.daily_loss>=MAX_DAILY_LOSS: return False,f"❌ Giới hạn lỗ ngày ${self.daily_loss:.2f}"
        return True,"✅"
    def record_result(self, pnl):
        self._reset(); self.daily_trades+=1
        if pnl>=0: self.daily_win_trades+=1; self.loss_streak=0
        else:
            self.daily_loss+=abs(pnl); self.loss_streak+=1
            if self.loss_streak>=3:
                from datetime import timedelta
                self.cooldown_until=datetime.now(VN_TZ)+timedelta(hours=1)
    def status_text(self):
        self._reset()
        return "\n".join([f"🛑 Emergency stop: {'BẬT' if self.emergency_stop else 'TẮT'}",
            f"📉 Lỗ hôm nay: ${self.daily_loss:.2f}/${MAX_DAILY_LOSS}",
            f"📊 Streak thua: {self.loss_streak}",
            f"⏳ Cooldown: {self.cooldown_until.strftime('%H:%M') if self.cooldown_until else 'Không'}",
            f"🔢 Trades: {self.daily_trades} (thắng {self.daily_win_trades})"])

class EMACrossover:
    def __init__(self,fast=9,slow=21):
        self.fast=fast;self.slow=slow;self.closes=[];self._pf=None;self._ps=None
    def _ema(self,p):
        if len(self.closes)<p: return None
        k=2/(p+1);e=self.closes[-p]
        for x in self.closes[-p+1:]: e=x*k+e*(1-k)
        return e
    def push_candle(self,close):
        self.closes.append(close)
        if len(self.closes)>self.slow*3: self.closes=self.closes[-(self.slow*3):]
        cf,cs=self._ema(self.fast),self._ema(self.slow);sig=None
        if cf and cs and self._pf and self._ps:
            if self._pf<self._ps and cf>cs: sig="BUY"
            elif self._pf>self._ps and cf<cs: sig="SELL"
        self._pf=cf;self._ps=cs;return sig

intents=discord.Intents.default()
bot=commands.Bot(command_prefix="!",intents=intents)
tree=bot.tree
risk=RiskManager();ema_strategies={};open_positions={};binance_client=None

def is_admin(i): return ADMIN_USER_ID==0 or i.user.id==ADMIN_USER_ID
def sym(s): s=s.upper();return s if s.endswith("USDT") else f"{s}USDT"
def rqty(qty,step): return Decimal(str(qty)).quantize(Decimal(str(step)),rounding=ROUND_DOWN)
async def gprice(symbol): return float((await binance_client.get_symbol_ticker(symbol=symbol.upper()))["price"])
async def gstep(symbol):
    info=await binance_client.get_symbol_info(symbol)
    for f in info["filters"]:
        if f["filterType"]=="LOT_SIZE": return float(f["stepSize"])
    return 0.00001
async def notify(msg):
    if DISCORD_CHANNEL_ID==0: return
    ch=bot.get_channel(DISCORD_CHANNEL_ID)
    if ch: await ch.send(msg)

@tree.command(name="price",description="Xem gia coin hien tai")
@app_commands.describe(symbol="Vi du BTC ETH BTCUSDT")
async def cmd_price(i:discord.Interaction,symbol:str):
    await i.response.defer()
    try:
        s=sym(symbol);p=await gprice(s)
        e=discord.Embed(title=f"💰 {s}",color=0x00b894)
        e.add_field(name="Giá",value=f"${p:,.4f} USDT")
        e.timestamp=datetime.now(timezone.utc)
        await i.followup.send(embed=e)
    except Exception as ex: await i.followup.send(f"❌ {ex}")

@tree.command(name="buy",description="Dat lenh MUA market order")
@app_commands.describe(symbol="Vi du BTC ETH",usdt_amount="So USDT vi du 50")
async def cmd_buy(i:discord.Interaction,symbol:str,usdt_amount:float):
    if not is_admin(i): await i.response.send_message("⛔",ephemeral=True);return
    await i.response.defer()
    s=sym(symbol);ok,reason=risk.check(usdt_amount)
    if not ok: await i.followup.send(f"🚫 {reason}");return
    try:
        p=await gprice(s);step=await gstep(s);qty=rqty(usdt_amount/p,step)
        if qty<=0: await i.followup.send("❌ Qty quá nhỏ.");return
        order=await binance_client.create_order(symbol=s,side="BUY",type="MARKET",quantity=str(qty))
        fp=float(order.get("fills",[{}])[0].get("price",p))
        sl=fp*(1-STOP_LOSS_PCT);tp=fp*(1+TAKE_PROFIT_PCT)
        open_positions[s]={"entry_price":fp,"qty":float(qty),"stop_loss":sl,"take_profit":tp,"strategy":"manual","order_id":order["orderId"]}
        sheet_append_order(s,"BUY",qty,fp,"FILLED",order["orderId"])
        sheet_append_position(s,fp,qty,sl,tp)
        e=discord.Embed(title="✅ Lệnh MUA thành công",color=0x00b894)
        e.add_field(name="Symbol",value=s,inline=True)
        e.add_field(name="Qty",value=str(qty),inline=True)
        e.add_field(name="Giá mua",value=f"${fp:,.4f}",inline=True)
        e.add_field(name="Stop Loss",value=f"${sl:,.4f} (-{STOP_LOSS_PCT*100:.0f}%)",inline=True)
        e.add_field(name="Take Profit",value=f"${tp:,.4f} (+{TAKE_PROFIT_PCT*100:.0f}%)",inline=True)
        e.timestamp=datetime.now(timezone.utc)
        await i.followup.send(embed=e)
    except BinanceAPIException as ex: await i.followup.send(f"❌ Binance: {ex.message}")
    except Exception as ex: log.exception("buy");await i.followup.send(f"❌ {ex}")

@tree.command(name="sell",description="Dat lenh BAN market order")
@app_commands.describe(symbol="Vi du BTC ETH")
async def cmd_sell(i:discord.Interaction,symbol:str):
    if not is_admin(i): await i.response.send_message("⛔",ephemeral=True);return
    await i.response.defer()
    s=sym(symbol)
    if s not in open_positions: await i.followup.send(f"❌ Không có position {s}.");return
    pos=open_positions[s]
    try:
        step=await gstep(s);qty=rqty(pos["qty"],step)
        order=await binance_client.create_order(symbol=s,side="SELL",type="MARKET",quantity=str(qty))
        sp=float(order.get("fills",[{}])[0].get("price",await gprice(s)))
        pnl=(sp-pos["entry_price"])*float(qty);pct=(sp/pos["entry_price"]-1)*100
        risk.record_result(pnl)
        sheet_append_order(s,"SELL",qty,sp,"FILLED",order["orderId"],pos["strategy"])
        sheet_append_position(s,pos["entry_price"],qty,pos["stop_loss"],pos["take_profit"],"CLOSED",pnl)
        del open_positions[s]
        icon="🟢" if pnl>=0 else "🔴"
        e=discord.Embed(title=f"{icon} Lệnh BÁN thành công",color=0x00b894 if pnl>=0 else 0xe74c3c)
        e.add_field(name="Symbol",value=s,inline=True)
        e.add_field(name="Giá bán",value=f"${sp:,.4f}",inline=True)
        e.add_field(name="PnL",value=f"${pnl:+.2f} ({pct:+.2f}%)",inline=True)
        e.timestamp=datetime.now(timezone.utc)
        await i.followup.send(embed=e)
    except BinanceAPIException as ex: await i.followup.send(f"❌ Binance: {ex.message}")
    except Exception as ex: log.exception("sell");await i.followup.send(f"❌ {ex}")

@tree.command(name="positions",description="Xem cac position dang mo")
async def cmd_positions(i:discord.Interaction):
    await i.response.defer()
    if not open_positions: await i.followup.send("📭 Không có position nào.");return
    e=discord.Embed(title="📊 Open Positions",color=0x6c5ce7)
    e.timestamp=datetime.now(timezone.utc)
    for s,pos in open_positions.items():
        try:
            cur=await gprice(s);pnl=(cur-pos["entry_price"])*pos["qty"];pct=(cur/pos["entry_price"]-1)*100
            icon="🟢" if pnl>=0 else "🔴"
            e.add_field(name=s,value=f"Vào: ${pos['entry_price']:,.4f}\nHiện: ${cur:,.4f}\nPnL: {icon}${pnl:+.2f}({pct:+.2f}%)\nSL:${pos['stop_loss']:,.4f}|TP:${pos['take_profit']:,.4f}",inline=False)
        except: e.add_field(name=s,value="lỗi giá",inline=False)
    await i.followup.send(embed=e)

@tree.command(name="portfolio",description="Xem so du Binance")
async def cmd_portfolio(i:discord.Interaction):
    if not is_admin(i): await i.response.send_message("⛔",ephemeral=True);return
    await i.response.defer(ephemeral=True)
    try:
        bals=[b for b in (await binance_client.get_account())["balances"] if float(b["free"])>0 or float(b["locked"])>0]
        e=discord.Embed(title="💼 Portfolio",color=0x6c5ce7)
        for b in bals[:20]:
            t=float(b["free"])+float(b["locked"])
            e.add_field(name=b["asset"],value=f"{t:.6f}"+(f"(🔒{float(b['locked']):.6f})" if float(b["locked"])>0 else ""),inline=True)
        if len(bals)>20: e.set_footer(text=f"+{len(bals)-20} khác")
        e.timestamp=datetime.now(timezone.utc)
        await i.followup.send(embed=e)
    except Exception as ex: await i.followup.send(f"❌ {ex}")

@tree.command(name="risk",description="Xem trang thai risk")
async def cmd_risk(i:discord.Interaction):
    if not is_admin(i): await i.response.send_message("⛔",ephemeral=True);return
    await i.response.send_message(f"🛡️ **Risk**\n{risk.status_text()}",ephemeral=True)

@tree.command(name="stop",description="Tat khan cap")
async def cmd_stop(i:discord.Interaction):
    if not is_admin(i): await i.response.send_message("⛔",ephemeral=True);return
    risk.emergency_stop=True
    await i.response.send_message("🛑 Emergency stop BẬT",ephemeral=True)
    await notify("🛑 **EMERGENCY STOP** kích hoạt!")

@tree.command(name="resume",description="Mo lai giao dich")
async def cmd_resume(i:discord.Interaction):
    if not is_admin(i): await i.response.send_message("⛔",ephemeral=True);return
    risk.emergency_stop=False
    await i.response.send_message("✅ Tiếp tục trading.",ephemeral=True)

@tree.command(name="strategy",description="Bat tat auto trading")
@app_commands.describe(symbol="BTC ETH",action="bat hoac tat",interval="1m 5m 1h 4h")
@app_commands.choices(action=[app_commands.Choice(name="bat",value="bat"),app_commands.Choice(name="tat",value="tat")])
async def cmd_strategy(i:discord.Interaction,symbol:str,action:str,interval:str="1h"):
    if not is_admin(i): await i.response.send_message("⛔",ephemeral=True);return
    s=sym(symbol)
    if action=="bat":
        ema_strategies[s]={"strategy":EMACrossover(),"interval":interval}
        await i.response.send_message(f"✅ Auto ON {s} EMA9/21 {interval}")
    else:
        ema_strategies.pop(s,None)
        await i.response.send_message(f"⏹️ Auto OFF {s}")

@tree.command(name="strategies",description="Danh sach strategy")
async def cmd_strategies(i:discord.Interaction):
    if not ema_strategies: await i.response.send_message("📭 Trống",ephemeral=True);return
    await i.response.send_message("⚙️ **Strategies:**\n"+"\n".join(f"• {s} EMA9/21 {v['interval']}" for s,v in ema_strategies.items()),ephemeral=True)

@tasks.loop(minutes=1)
async def auto_trade_loop():
    if risk.emergency_stop: return
    for s,pos in list(open_positions.items()):
        try:
            p=await gprice(s)
            if p<=pos["stop_loss"] or p>=pos["take_profit"]:
                label="STOP LOSS" if p<=pos["stop_loss"] else "TAKE PROFIT"
                step=await gstep(s);qty=rqty(pos["qty"],step)
                order=await binance_client.create_order(symbol=s,side="SELL",type="MARKET",quantity=str(qty))
                sp=float(order.get("fills",[{}])[0].get("price",p))
                pnl=(sp-pos["entry_price"])*float(qty)
                risk.record_result(pnl)
                sheet_append_order(s,"SELL",qty,sp,"FILLED",order["orderId"],pos.get("strategy","auto"))
                sheet_append_position(s,pos["entry_price"],qty,pos["stop_loss"],pos["take_profit"],"CLOSED",pnl)
                del open_positions[s]
                await notify(f"{'🟢' if pnl>=0 else '🔴'} **{label}** {s} ${sp:,.4f} PnL:${pnl:+.2f}")
        except Exception as e: log.error(f"SL/TP {s}:{e}")
    for s,cfg in list(ema_strategies.items()):
        try:
            candles=await binance_client.get_klines(symbol=s,interval=cfg["interval"],limit=30)
            sig=None
            for c in candles[:-1]: sig=cfg["strategy"].push_candle(float(c[4]))
            if sig=="BUY" and s not in open_positions:
                p=await gprice(s);ok,_=risk.check(MAX_POSITION_USDT*0.5)
                if ok:
                    step=await gstep(s);qty=rqty((MAX_POSITION_USDT*0.5)/p,step)
                    order=await binance_client.create_order(symbol=s,side="BUY",type="MARKET",quantity=str(qty))
                    fp=float(order.get("fills",[{}])[0].get("price",p))
                    sl=fp*(1-STOP_LOSS_PCT);tp=fp*(1+TAKE_PROFIT_PCT)
                    open_positions[s]={"entry_price":fp,"qty":float(qty),"stop_loss":sl,"take_profit":tp,"strategy":"ema","order_id":order["orderId"]}
                    sheet_append_order(s,"BUY",qty,fp,"FILLED",order["orderId"],"ema")
                    sheet_append_position(s,fp,qty,sl,tp)
                    await notify(f"🤖 AUTO BUY {s} golden cross ${fp:,.4f} SL:${sl:,.4f} TP:${tp:,.4f}")
            elif sig=="SELL" and s in open_positions:
                pos=open_positions[s];step=await gstep(s);qty=rqty(pos["qty"],step)
                order=await binance_client.create_order(symbol=s,side="SELL",type="MARKET",quantity=str(qty))
                sp=float(order.get("fills",[{}])[0].get("price",await gprice(s)))
                pnl=(sp-pos["entry_price"])*float(qty)
                risk.record_result(pnl)
                sheet_append_order(s,"SELL",qty,sp,"FILLED",order["orderId"],"ema")
                sheet_append_position(s,pos["entry_price"],qty,pos["stop_loss"],pos["take_profit"],"CLOSED",pnl)
                del open_positions[s]
                await notify(f"🤖 AUTO SELL {s} death cross ${sp:,.4f} PnL:{'🟢' if pnl>=0 else '🔴'}${pnl:+.2f}")
        except Exception as e: log.error(f"Strategy {s}:{e}")

@tasks.loop(hours=24)
async def daily_report():
    wr=(risk.daily_win_trades/risk.daily_trades*100) if risk.daily_trades>0 else 0
    sheet_append_pnl(risk.daily_loss*-1,risk.daily_trades,risk.daily_win_trades)
    await notify(f"📋 **Báo cáo {datetime.now(VN_TZ).strftime('%d/%m/%Y')}**\nTrades:{risk.daily_trades} thắng:{risk.daily_win_trades} winrate:{wr:.0f}%\nPnL:${risk.daily_loss*-1:+.2f}")

@bot.event
async def on_ready():
    global binance_client
    log.info(f"Bot: {bot.user}")
    binance_client=await AsyncClient.create(api_key=BINANCE_API_KEY,api_secret=BINANCE_API_SECRET)
    log.info("Binance OK")
    await tree.sync()
    log.info("Commands synced")
    if not auto_trade_loop.is_running(): auto_trade_loop.start()
    if not daily_report.is_running():
        from datetime import timedelta
        now=datetime.now(VN_TZ);target=now.replace(hour=7,minute=0,second=0,microsecond=0)
        if now>=target: target+=timedelta(days=1)
        bot.loop.call_later((target-now).total_seconds(),daily_report.start)
    await notify(f"🚀 Bot online {datetime.now(VN_TZ).strftime('%d/%m/%Y %H:%M')}")

bot.run(DISCORD_TOKEN)

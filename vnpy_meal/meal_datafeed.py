from datetime import datetime, timedelta, time
from typing import List, Optional, Callable

from pandas import DataFrame

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.utility import ZoneInfo
import efinance as ef

EXCHANGE_VT2UDATA = {
    Exchange.CFFEX: "CFE",
    Exchange.SHFE: "SHF",
    Exchange.DCE: "DCE",
    Exchange.CZCE: "CZC",
    Exchange.INE: "INE",
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.SEHK: "HK"
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")

FUTURE_EXCHANGES: list = [Exchange.CFFEX, Exchange.SHFE, Exchange.DCE, Exchange.CZCE, Exchange.INE]


def convert_symbol(symbol: str, exchange: Exchange) -> str:
    """将交易所代码转换为Meal代码"""
    return f"{symbol.upper()}"


class MealDatafeed(BaseDatafeed):
    """Meal数据服务接口"""

    def __init__(self):
        """"""
        self.token: str = SETTINGS["datafeed.password"]

        self.inited: bool = False

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        if not self.inited:
            self.init()
        data: List[BarData] = []
        end: datetime = req.end

        # 只支持分钟线
        if req.interval not in (Interval.MINUTE, Interval.DAILY, Interval.HOUR):
            output("数据服务获取K线数据失败：目前只支持1分钟,60分钟,1天周期K线！")
            return None

        while True:
            if req.exchange in EXCHANGE_VT2UDATA:
                temp_data = self.query_bar_data(req)
                if temp_data:
                    data.extend(temp_data)
                    if temp_data[0].datetime.date() == temp_data[-1].datetime.date():
                        break
                    req.start = temp_data[-1].datetime + timedelta(days=1)
                else:
                    if req.end >= end:
                        return data
                    else:
                        req.start = req.end + timedelta(days=1)
            else:
                output(f"UData数据服务获取K线数据失败：不支持的交易所{req.exchange.value}！")
                return None
        return data

    def query_bar_data(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询分钟K线数据"""
        symbol = req.symbol
        exchange = req.exchange
        start = req.start
        interval = req.interval

        adjustment = timedelta(minutes=1)
        inter_value = 1
        if req.interval == req.interval.MINUTE:
            inter_value = 1
        elif req.interval == req.interval.HOUR:
            inter_value = 60
        elif req.interval == req.interval.DAILY:
            inter_value = 101
        elif req.interval == req.interval.WEEKLY:
            inter_value = 102
        elif req.interval == req.interval.MINUTE:
            inter_value = 103
        elif req.interval == req.interval.TICK:
            inter_value = 1
        else:
            pass
        df: DataFrame = ef.stock.get_quote_history(stock_codes=symbol,
                                                   beg=start.strftime("%Y%m%d"),
                                                   end=req.end.strftime("%Y%m%d"),
                                                   klt=inter_value)

        data: List[BarData] = []

        if len(df):
            for _, row in df.iterrows():
                try:
                    dt = datetime.strptime(row["日期"], "%Y-%m-%d %H:%M") - adjustment
                except:
                    dt = datetime.strptime(row["日期"], "%Y-%m-%d") - adjustment
                dt = dt.replace(tzinfo=CHINA_TZ)
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=row["开盘"],
                    high_price=row["最高"],
                    low_price=row["最低"],
                    close_price=row["收盘"],
                    volume=row["成交量"],
                    turnover=row["成交额"],
                    gateway_name="MEAL"
                )
                if req.exchange in FUTURE_EXCHANGES:
                    bar.open_interest = row.amount
                else:
                    if dt.time() == time(hour=9, minute=29):
                        continue

                data.append(bar)

        return data

import re
import os
import gc
import time
import py7zr
import zipfile
import tempfile
import datetime
import numpy as np
import pandas as pd
import multiprocessing as mp
from typing import Union, Tuple
from .db.mysql_db import MySQLDB
from .db.clickhouse_db import *
from .config import dir_datayes_data, dir_kuanrui_data, dir_mdatayes_data, dir_unzip_tmp, g_datayes_password
from keynes.common.log import configure_logging
from concurrent.futures import ProcessPoolExecutor

g_logger = configure_logging(__name__, with_console=True)

ORDER_COLUMNS = ['TradingDay', 'Code', 'Time', 'UpdateTime', 'OrderID', 'Side', 'Price', 'Volume', 'OrderType', 'Channel', 'SeqNum']
DEAL_COLUMNS = ['TradingDay', 'Code', 'Time', 'UpdateTime', 'SaleOrderID', 'BuyOrderID', 'Side', 'Price', 'Volume', 'Money', 'Channel', 'SeqNum']
TICK_COLUMNS = [
    'TradingDay', 'Code', 'Time', 'UpdateTime', 'CurrentPrice', 'TotalVolume', 'TotalMoney', 'PreClosePrice', 'OpenPrice', 
    'HighestPrice', 'LowestPrice', 'HighLimitPrice', 'LowLimitPrice', 'IOPV', 'TradeNum', 'TotalBidVolume', 'TotalAskVolume', 'AvgBidPrice', 'AvgAskPrice',
    'AskPrice1', 'AskPrice2', 'AskPrice3', 'AskPrice4', 'AskPrice5', 'AskPrice6', 'AskPrice7', 'AskPrice8', 'AskPrice9', 'AskPrice10',
    'AskVolume1', 'AskVolume2', 'AskVolume3', 'AskVolume4', 'AskVolume5', 'AskVolume6', 'AskVolume7', 'AskVolume8', 'AskVolume9', 'AskVolume10', 
    'AskNum1', 'AskNum2', 'AskNum3', 'AskNum4', 'AskNum5', 'AskNum6', 'AskNum7', 'AskNum8', 'AskNum9', 'AskNum10',
    'BidPrice1', 'BidPrice2', 'BidPrice3', 'BidPrice4', 'BidPrice5', 'BidPrice6', 'BidPrice7', 'BidPrice8', 'BidPrice9', 'BidPrice10',
    'BidVolume1', 'BidVolume2', 'BidVolume3', 'BidVolume4', 'BidVolume5', 'BidVolume6', 'BidVolume7', 'BidVolume8', 'BidVolume9', 'BidVolume10', 
    'BidNum1', 'BidNum2', 'BidNum3', 'BidNum4', 'BidNum5', 'BidNum6', 'BidNum7', 'BidNum8', 'BidNum9', 'BidNum10',
    "Channel", "SeqNum"]


def check_invalid_data(df_data, data_type):

    if df_data.empty:
        return 0

    dict_common_dtype = {
        "TradingDay": np.dtype('object'),
        "Code": np.dtype('object'),
        "Time": np.dtype('datetime64[ns]'),
        "UpdateTime": np.dtype('datetime64[ns]'),
        "Channel": np.dtype('int64'),
        "SeqNum": np.dtype('int64'),
    }

    dict_order_dtype = dict_common_dtype.copy()
    dict_order_dtype.update({
        "OrderID": np.dtype('int64'),
        "Side": np.dtype('int16'),
        "Price": np.dtype('float64'),
        "Volume": np.dtype('float64'),
        "OrderType": np.dtype('int16'),
        })

    dict_deal_dtype = dict_common_dtype.copy()
    dict_deal_dtype.update({
        "SaleOrderID": np.dtype('int64'),
        "BuyOrderID": np.dtype('int64'),
        "Side": np.dtype('int16'),
        "Price": np.dtype('float64'),
        "Volume": np.dtype('float64'),
        "Money": np.dtype('float64'),
        })

    dict_tick_dtype = dict_common_dtype.copy()
    dict_tick_dtype.update(
        {i: np.dtype('float64') for i in pd.Index(TICK_COLUMNS).difference(list(dict_tick_dtype.keys()))})

    dict_type = {"order": dict_order_dtype, "deal": dict_deal_dtype, "tick": dict_tick_dtype}
    valid = pd.Series(dict_type[data_type]).sort_index().equals(df_data.dtypes.sort_index())

    valid = valid and (not df_data[["Channel", "SeqNum"]].duplicated().any())
    code = df_data["Code"].values[0]
    pattern_sz, pattern_sh = r'^[03]\d{5}\.XSHE$', r'^[6]\d{5}\.XSHG$'
    valid = valid and bool(re.fullmatch(pattern_sz, code) or re.fullmatch(pattern_sh, code))
    
    if data_type == "order":
        valid = valid and df_data["Side"].isin([0, 1]).all()
        valid = valid and np.isfinite(df_data[["Price", "Volume"]]).all().all()
        if code.endswith("XSHE"):
            valid = valid and df_data["OrderType"].isin([1, 2, 3]).all()
        else:
            valid = valid and df_data["OrderType"].isin([2, 5]).all()
    elif data_type == "deal":
        valid = valid and np.isfinite(df_data[["Price", "Volume", "Money"]]).all().all()
        if code.endswith("XSHE"):
            valid = valid and df_data["Side"].isin([0, 1, 10, 4]).all()
        else:
            valid = valid and df_data["Side"].isin([0, 1, 10]).all()
    else:
        finite_fields = [f"{i}{j}{k}" for i in ["Bid", "Ask"] for j in ["Price", "Volume", "Num"] for k in range(1, 11)]
        valid = valid and np.isfinite(df_data[finite_fields]).all().all()
    
    return int(not valid)


def convert_sh_order_hist(df_order: pd.DataFrame, trading_day: Union[pd.Timestamp, str]) -> pd.DataFrame:
    
    # 上交所逐笔委托的历史数据(20210620及之前)
    trading_day = pd.Timestamp(trading_day)
    dict_rename = {
        "SecurityID": "Code",
        "OrderTime": "Time",
        "OrderNO": "OrderID",
        "OrderBSFlag": "Side",
        "OrderPrice": "Price",
        "Balance": "Volume",
        "OrderChannel": "Channel",
        "BizIndex": "SeqNum",
    }

    df_order = df_order.rename(columns=dict_rename)
    df_order["TradingDay"] = trading_day.date()
    df_order["Code"] = df_order["Code"].astype(str).str.zfill(6) + ".XSHG"
    df_order["Time"] = pd.to_datetime(df_order["Time"], format="%Y%m%d%H%M%S%f")
    df_order["UpdateTime"] = df_order["Time"]
    df_order['Side'] = df_order["Side"].map({"B": 0, "S": 1}).astype("int16")
    df_order["OrderType"] = df_order["OrderType"].map({"A": 2, "D": 5}).astype("int16")
    
    # astype
    fields_int64 = ["OrderID", "Channel", "SeqNum"]
    df_order[fields_int64] = df_order[fields_int64].astype("int64")
    fields_float64 = ["Price", "Volume"]
    df_order[fields_float64] = df_order[fields_float64].astype("float64")

    df_order = df_order[ORDER_COLUMNS].sort_values(by='SeqNum').reset_index(drop=True)
    
    return df_order


def convert_sh_order_old(df_order: pd.DataFrame, trading_day: Union[pd.Timestamp, str]) -> pd.DataFrame:

    trading_day = pd.Timestamp(trading_day)
    dict_rename = {
        "SecurityID": "Code",
        "OrderTime": "Time",
        "LocalTime": "UpdateTime",
        "OrderNO": "OrderID",
        "OrderBSFlag": "Side",
        "OrderPrice": "Price",
        "Balance": "Volume",
        "OrderChannel": "Channel",
        "BizIndex": "SeqNum",
    }
    
    df_order = df_order.rename(columns=dict_rename)
    df_order["TradingDay"] = trading_day.date()
    df_order["Code"] = df_order["Code"].astype(str).str.zfill(6) + ".XSHG"
    df_order["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_order["Time"])
    df_order["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_order["UpdateTime"])
    df_order['Side'] = df_order["Side"].map({"B": 0, "S": 1}).astype("int16")
    df_order["OrderType"] = df_order["OrderType"].map({"A": 2, "D": 5}).astype("int16")
    # astype
    fields_int64 = ["OrderID", "Channel", "SeqNum"]
    df_order[fields_int64] = df_order[fields_int64].astype("int64")
    fields_float64 = ["Price", "Volume"]
    df_order[fields_float64] = df_order[fields_float64].astype("float64")

    df_order = df_order[ORDER_COLUMNS].sort_values(by='SeqNum').reset_index(drop=True)

    return df_order


def convert_sh_deal_old(df_deal: pd.DataFrame, trading_day: Union[pd.Timestamp, str]) -> pd.DataFrame:

    trading_day = pd.Timestamp(trading_day)
    dict_rename = {
        "SecurityID": "Code",
        "TradTime": "Time",
        "LocalTime": "UpdateTime",
        "TradeSellNo": "SaleOrderID",
        "TradeBuyNo": "BuyOrderID",
        "TradeBSFlag": "Side",
        "TradPrice": "Price",
        "TradVolume": "Volume",
        "TradeMoney": "Money",
        "TradeChan": "Channel",
        "BizIndex": "SeqNum",
    }
    df_deal = df_deal.rename(columns=dict_rename)
    df_deal["TradingDay"] = trading_day.date()
    df_deal["Code"] = df_deal["Code"].astype(str).str.zfill(6) + ".XSHG"
    df_deal["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_deal["Time"])
    df_deal["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_deal["UpdateTime"])
    df_deal["Side"] = df_deal["Side"].map({"N": 10, "B": 0, 'S': 1}).astype('int16')
    if trading_day < pd.Timestamp("20210426"):        
        # 20210426之前，使用TradeIndex作为SeqNum，20210426及之后使用BizIndex作为SeqNum
        df_deal["SeqNum"] = df_deal["TradeIndex"]
    fields_int64 = ["SaleOrderID", "BuyOrderID", "Channel", "SeqNum"]
    df_deal[fields_int64] = df_deal[fields_int64].astype("int64")
    fields_float64 = ["Price", "Volume", "Money"]
    df_deal[fields_float64] = df_deal[fields_float64].astype("float64")

    df_deal = df_deal[DEAL_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

    return df_deal


def convert_sh_order_deal(df_data: pd.DataFrame, trading_day: Union[pd.Timestamp, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    df_order = df_data[df_data["Type"].isin(["A", "D"])].copy()
    df_deal = df_data[df_data["Type"] == "T"].copy()
    
    trading_day = pd.Timestamp(trading_day)
    
    dict_rename_order = {
        "SecurityID": "Code",
        "TickTime": "Time",
        "LocalTime": "UpdateTime",
        "TickBSFlag": "Side",
        "Qty": "Volume",
        "Type": "OrderType",
        "BizIndex": "SeqNum",
    }
    df_order = df_order.rename(columns=dict_rename_order)
    df_order["TradingDay"] = trading_day.date()
    df_order["Code"] = df_order["Code"].astype(str).str.zfill(6) + ".XSHG"
    df_order["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_order["Time"])
    df_order["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_order["UpdateTime"])
    df_order["OrderID"] = df_order["BuyOrderNO"] + df_order["SellOrderNO"]
    df_order['Side'] = df_order["Side"].map({"B": 0, "S": 1}).astype("int16")
    df_order['OrderType'] = df_order["OrderType"].map({"A": 2, "D": 5}).astype('int16')
    # astype
    fields_int64 = ["OrderID", "Channel", "SeqNum"]
    df_order[fields_int64] = df_order[fields_int64].astype("int64")
    fields_float64 = ["Price", "Volume"]
    df_order[fields_float64] = df_order[fields_float64].astype("float64")

    df_order = df_order[ORDER_COLUMNS].sort_values('SeqNum').reset_index(drop=True)
    
    if df_deal.empty:
        df_deal = pd.DataFrame()
    else:
        dict_rename_deal = {
            "SecurityID": "Code",
            "TickTime": "Time",
            "LocalTime": "UpdateTime",
            "SellOrderNO": "SaleOrderID",
            "BuyOrderNO": "BuyOrderID",
            "TickBSFlag": "Side",
            "Qty": "Volume",
            "TradeMoney": "Money",
            "BizIndex": "SeqNum",
        }
        df_deal = df_deal.rename(columns=dict_rename_deal)
        df_deal["TradingDay"] = trading_day.date()
        df_deal["Code"] = df_deal["Code"].astype(str).str.zfill(6) + ".XSHG"
        df_deal["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_deal["Time"])
        df_deal["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_deal["UpdateTime"])
        df_deal["Side"] = df_deal["Side"].map({"N": 10, "B": 0, 'S': 1}).astype('int16')
        # astype
        fields_int64 = ["SaleOrderID", "BuyOrderID", "Channel", "SeqNum"]
        df_deal[fields_int64] = df_deal[fields_int64].astype("int64")
        fields_float64 = ["Price", "Volume", "Money"]
        df_deal[fields_float64] = df_deal[fields_float64].astype("float64")
        
        df_deal = df_deal[DEAL_COLUMNS].sort_values('SeqNum').reset_index(drop=True)

    return df_order, df_deal


def convert_sz_order(df_order: pd.DataFrame, trading_day: Union[pd.Timestamp, str]) -> pd.DataFrame:
    
    trading_day = pd.Timestamp(trading_day)
    dict_rename = {
        "SecurityID": "Code",
        "TransactTime": "Time",
        "LocalTime": "UpdateTime",
        "ApplSeqNum": "OrderID",
        "OrderQty": "Volume",
        "OrdType": "OrderType",
        "ChannelNo": "Channel",
    }

    df_order = df_order.rename(columns=dict_rename)
    df_order["TradingDay"] = trading_day.date()
    df_order["Code"] = df_order["Code"].astype(str).str.zfill(6) + ".XSHE"
    df_order["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_order["Time"])
    df_order["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_order["UpdateTime"])
    df_order['Side'] = df_order["Side"].map({49: 0, 50: 1}).astype("int16")
    df_order['OrderType'] = df_order["OrderType"].map({49: 1, 50: 2, 85: 3}).astype('int16')
    df_order["SeqNum"] = df_order["OrderID"]
    # astype
    fields_int64 = ["OrderID", "Channel", "SeqNum"]
    df_order[fields_int64] = df_order[fields_int64].astype("int64")
    fields_float64 = ["Price", "Volume"]
    df_order[fields_float64] = df_order[fields_float64].astype("float64")

    df_order = df_order[ORDER_COLUMNS].sort_values(by='SeqNum').reset_index(drop=True)

    return df_order


def convert_sz_deal(df_deal: pd.DataFrame, trading_day: Union[pd.Timestamp, str]) -> pd.DataFrame:

    trading_day = pd.Timestamp(trading_day)
    dict_rename = {
        "SecurityID": "Code",
        "TransactTime": "Time",
        "LocalTime": "UpdateTime",
        "OfferApplSeqNum": "SaleOrderID",
        "BidApplSeqNum": "BuyOrderID",
        "LastPx": "Price",
        "LastQty": "Volume",
        "ChannelNo": "Channel",
        "ApplSeqNum": "SeqNum",
    }
        
    df_deal = df_deal.rename(columns=dict_rename)
    df_deal["TradingDay"] = trading_day.date()
    df_deal["Code"] = df_deal["Code"].astype(str).str.zfill(6) + ".XSHE"
    df_deal["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_deal["Time"])
    df_deal["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_deal["UpdateTime"])
    df_deal["Side"] = np.int16(1)
    df_deal.loc[df_deal["BuyOrderID"] > df_deal["SaleOrderID"], 'Side'] = 0
    df_deal.loc[df_deal["ExecType"] == 52, 'Side'] = 4
    df_deal["Money"] = df_deal["Price"] * df_deal["Volume"]
    # astype
    fields_int64 = ["SaleOrderID", "BuyOrderID", "Channel", "SeqNum"]
    df_deal[fields_int64] = df_deal[fields_int64].astype("int64")
    fields_float64 = ["Price", "Volume", "Money"]
    df_deal[fields_float64] = df_deal[fields_float64].astype("float64")

    df_deal = df_deal[DEAL_COLUMNS].sort_values(by='SeqNum').reset_index(drop=True)

    return df_deal


def convert_sh_tick(df_tick: pd.DataFrame, trading_day: Union[pd.Timestamp, str], 
                    high_limit: float = 0.0, low_limit: float =0.0) -> pd.DataFrame:
    
    trading_day = pd.Timestamp(trading_day)
    dict_rename = {
        "SecurityID": "Code",
        "UpdateTime": "Time",
        "LocalTime": "UpdateTime",
        "LastPrice": "CurrentPrice",
        "TradVolume": "TotalVolume",
        "Turnover": "TotalMoney",
        "PreCloPrice": "PreClosePrice",
        "HighPrice": "HighestPrice",
        "LowPrice": "LowestPrice",
        "TradNumber": "TradeNum",
        "TotalBidVol": "TotalBidVolume",
        "TotalAskVol": "TotalAskVolume",
        "WAvgBidPri": "AvgBidPrice",
        "WAvgAskPri": "AvgAskPrice",
        "NumOrdersB1": "BidNum1", "NumOrdersB2": "BidNum2", "NumOrdersB3": "BidNum3", "NumOrdersB4": "BidNum4", "NumOrdersB5": "BidNum5",
        "NumOrdersB6": "BidNum6", "NumOrdersB7": "BidNum7", "NumOrdersB8": "BidNum8", "NumOrdersB9": "BidNum9", "NumOrdersB10": "BidNum10",
        "NumOrdersS1": "AskNum1", "NumOrdersS2": "AskNum2", "NumOrdersS3": "AskNum3", "NumOrdersS4": "AskNum4", "NumOrdersS5": "AskNum5",
        "NumOrdersS6": "AskNum6", "NumOrdersS7": "AskNum7", "NumOrdersS8": "AskNum8", "NumOrdersS9": "AskNum9", "NumOrdersS10": "AskNum10",
        "SeqNo": "SeqNum",
    }
    df_tick = df_tick.rename(columns=dict_rename)
    df_tick["TradingDay"] = trading_day.date()
    df_tick["Code"] = df_tick["Code"].astype(str).str.zfill(6) + ".XSHG"
    df_tick["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_tick["Time"])
    df_tick["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_tick["UpdateTime"])
    df_tick["HighLimitPrice"] = high_limit
    df_tick["LowLimitPrice"] = low_limit
    df_tick["Channel"] = np.int64(0)
    df_tick["SeqNum"] = df_tick["SeqNum"].astype("int64")
    
    if trading_day < pd.Timestamp(2019, 6, 6):
        for i_col in [f"{i}Num{j}" for i in ["Ask", "Bid"] for j in range(1, 11)]:
            if i_col not in df_tick.columns:
                df_tick[i_col] = 0.0
    
    columns_float64 = pd.Index(TICK_COLUMNS).difference(["TradingDay", "Code", "Time", "UpdateTime", "Channel", "SeqNum"])
    df_tick[columns_float64] = df_tick[columns_float64].astype("float64").fillna(0.0) # fillna(0.0): jq的处理方法
    
    df_tick = df_tick[TICK_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

    return df_tick


def convert_sz_tick(df_tick: pd.DataFrame, trading_day: Union[pd.Timestamp, str]) -> pd.DataFrame:
    
    trading_day = pd.Timestamp(trading_day)
    dict_rename = {
        "SecurityID": "Code",
        "UpdateTime": "Time",
        "LocalTime": "UpdateTime",
        "LastPrice": "CurrentPrice",
        "Volume": "TotalVolume",
        "Turnover": "TotalMoney",
        "PreCloPrice": "PreClosePrice",
        "HighPrice": "HighestPrice",
        "LowPrice": "LowestPrice",
        "TurnNum": "TradeNum",
        "TotalBidQty": "TotalBidVolume",
        "TotalOfferQty": "TotalAskVolume",
        "WeightedAvgBidPx": "AvgBidPrice",
        "WeightedAvgOfferPx": "AvgAskPrice",
        "NumOrdersB1": "BidNum1", "NumOrdersB2": "BidNum2", "NumOrdersB3": "BidNum3", "NumOrdersB4": "BidNum4", "NumOrdersB5": "BidNum5",
        "NumOrdersB6": "BidNum6", "NumOrdersB7": "BidNum7", "NumOrdersB8": "BidNum8", "NumOrdersB9": "BidNum9", "NumOrdersB10": "BidNum10",
        "NumOrdersS1": "AskNum1", "NumOrdersS2": "AskNum2", "NumOrdersS3": "AskNum3", "NumOrdersS4": "AskNum4", "NumOrdersS5": "AskNum5",
        "NumOrdersS6": "AskNum6", "NumOrdersS7": "AskNum7", "NumOrdersS8": "AskNum8", "NumOrdersS9": "AskNum9", "NumOrdersS10": "AskNum10",
        "SeqNo": "SeqNum",
    }
    df_tick = df_tick.rename(columns=dict_rename)
    df_tick["TradingDay"] = trading_day.date()
    df_tick["Code"] = df_tick["Code"].astype(str).str.zfill(6) + ".XSHE"
    df_tick["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_tick["Time"])
    df_tick["UpdateTime"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_tick["UpdateTime"])
    df_tick["Channel"] = np.int64(0)
    df_tick["SeqNum"] = df_tick["SeqNum"].astype("int64")
    if trading_day < pd.Timestamp(2019, 6, 6):
        for i_col in [f"{i}Num{j}" for i in ["Ask", "Bid"] for j in range(1, 11)]:
            if i_col not in df_tick.columns:
                df_tick[i_col] = 0.0
    columns_float64 = pd.Index(TICK_COLUMNS).difference(["TradingDay", "Code", "Time", "UpdateTime", "Channel", "SeqNum"])
    df_tick[columns_float64] = df_tick[columns_float64].astype("float64")

    df_tick = df_tick[TICK_COLUMNS].sort_values("SeqNum").reset_index(drop=True).fillna(0.0) # fillna(0.0): jq的处理方法
    
    return df_tick


def save_to_clickhouse(ck_db : ClickhouseDB, df_data: pd.DataFrame, data_type: str) -> int:
    assert data_type in ["order", "deal", "tick"]
    #重试几次
    if data_type == "order":
        ret = ck_db.insert_stock_order(df_data)
    elif data_type == "deal":
        ret = ck_db.insert_stock_deal(df_data)
    elif data_type == "tick":
        ret = ck_db.insert_stock_snap(df_data)
    else:
        ret = False
    failed = 0 if ret else 1
    if failed:
        g_logger.warning(f"Saved data to Clickhouse: Failed: {failed} TradingDay: {df_data['TradingDay'].values[0].strftime('%Y-%m-%d')}, {data_type}, code: {df_data['Code'].values[0]}, shape: {df_data.shape}")
    return failed


def log_to_clickhouse(ck_db : ClickhouseDB, df_log: pd.DataFrame):
    # Time(), TradingDay(Date), Code(FixedString(11)), DataType(String), RowNum(int64), ConvertFailed(int64), ConvertError(String), InvalidData(int64), SaveFailed(int64)
    ck_db.insert_convert_info(df_log)


def read_zip_data(dir_t, code=None):

    """
    Linux的unzip比python的快很多，
    对于datayes和mm的数据，使用linux的unzip解压到dir_unzip_tmp，读取后再删除
    kuanrui的数据暂时不需要这么操作
    """

    # _t = time.time()
    if not os.path.exists(dir_t):
        raise ValueError(f"file not exist: {dir_t}")

    if dir_t.startswith(dir_datayes_data):
        chunksize = int(2e6) if "6_28_0.csv" in dir_t or "MarketData" in dir_t else int(2e7)
        with zipfile.ZipFile(dir_t, 'r') as zip_ref:
            # 检查是否有文件被加密
            is_encrypted = any(zip_info.flag_bits & 0x1 for zip_info in zip_ref.filelist)
            file_name = zip_ref.namelist()[0]
            dir_unzip_t = os.path.join(dir_unzip_tmp, dir_t.split("/")[-1].split(".")[0])
            
            if os.path.exists(dir_unzip_t):
                os.system(f"rm -r {dir_unzip_t}")
            os.mkdir(dir_unzip_t)

            if is_encrypted:
                os.system(f'unzip -q -P {g_datayes_password.decode("utf-8")} {dir_t} -d {dir_unzip_t}')
            else:
                os.system(f'unzip -q {dir_t} -d {dir_unzip_t}')

        dir_unzip_t2 = os.path.join(dir_unzip_t, file_name)
        chunk_iter = pd.read_csv(dir_unzip_t2, index_col=False, chunksize=chunksize)
        df_data = pd.DataFrame()
        for i_num, chunk in enumerate(chunk_iter):
            df_data = pd.concat([df_data, chunk], ignore_index=True)
            if i_num % 2 == 0:
                gc.collect()

        os.system(f"rm -r {dir_unzip_t}")

    elif dir_t.startswith(dir_kuanrui_data):
        dict_type = {
            "SecurityID": "int64", "OrderTime": "str", "OrderNO": "int64", "OrderPrice": "float64", "Balance": "float64", 
            "OrderBSFlag": "str", "OrderType": "str", "OrderIndex": "int64", "OrderChannel": "int64", "BizIndex": "int64"}
        columns_name = ["SecurityID", "OrderTime", "OrderNO", "OrderPrice", "Balance", "OrderBSFlag", "OrderType", "OrderIndex", "OrderChannel", "BizIndex"]
        
        with tempfile.TemporaryDirectory() as temp_dir:  # 临时目录，退出上下文自动删除
            with py7zr.SevenZipFile(dir_t, mode='r') as z:
                z.extractall(temp_dir)
            csv_path = os.path.join(temp_dir, "Entrust.csv")  # 读取临时目录中的文件

            chunk_iter = pd.read_csv(csv_path, skiprows=[0], header=None, names=columns_name, chunksize=int(2e7))
            df_data = pd.DataFrame()
            for i_num, chunk in enumerate(chunk_iter):
                chunk = chunk.astype(dict_type)
                df_data = pd.concat([df_data, chunk], ignore_index=True)
                if i_num % 2 == 0:
                    gc.collect()
    
    elif dir_t.startswith(dir_mdatayes_data):
        
        assert code is not None
        dir_unzip_t = os.path.join(dir_unzip_tmp, f"{dir_t.split("/")[-3]}_{dir_t.split("/")[-1].split("_")[0]}_{code.split('.')[0]}")
        if os.path.exists(dir_unzip_t):
            os.system(f"rm -r {dir_unzip_t}")
        os.mkdir(dir_unzip_t)

        os.system(f"unzip -q {dir_t} {code.split('.')[0]}.csv -d {dir_unzip_t}")
        dir_unzip_t2 = os.path.join(dir_unzip_t, f"{code.split('.')[0]}.csv" )
        df_data = pd.read_csv(dir_unzip_t2)
        os.system(f"rm -r {dir_unzip_t}")
    
    else:
        raise ValueError(f"illegal file_dir: {dir_t}")

    # g_logger.info(f"read zip data use seconds: {time.time() - _t}, file_name: {dir_t}")

    return df_data


def convert_data_(ck_db : ClickhouseDB, trading_day: pd.Timestamp, security_id: int, df_data: pd.DataFrame, data_type : str, convert_fun, dict_price: dict):

    code = str(security_id).zfill(6)
    code = code + {"0": ".XSHE", "3": ".XSHE", "6": ".XSHG"}[code[0]]

    dict_log = {"Time": pd.Timestamp.now(), "TradingDay": trading_day.date(), "Code": code, "RowNum": 0, 
                "ConvertFailed": 0, "ConvertError": "", "InvalidData": 0}

    dict_fun_data_type = {
            "convert_sh_order_hist": "order", "convert_sh_order_old": "order", "convert_sz_order": "order",
            "convert_sh_deal_old": "deal", "convert_sz_deal": "deal",
            "convert_sh_tick": "tick", "convert_sz_tick": "tick"}
    
    if convert_fun.__name__ == "convert_sh_order_deal":
        df_order, df_deal = pd.DataFrame(), pd.DataFrame()
        dict_log_order, dict_log_deal = dict_log.copy(), dict_log.copy()
        if not df_data.empty:
            try:
                df_order, df_deal = convert_fun(df_data, trading_day)
                invalid_order, invalid_deal = check_invalid_data(df_order, "order"), check_invalid_data(df_deal, "deal")
                dict_log_order.update({"RowNum": df_order.shape[0], "InvalidData": invalid_order})
                dict_log_deal.update({"RowNum": df_deal.shape[0], "InvalidData": invalid_deal})
            except Exception as e:
                g_logger.error(f"error in {convert_fun.__name__}, {trading_day}, {code}")
                dict_log_order.update({"ConvertFailed": 1, "ConvertError": e})
                dict_log_deal.update({"ConvertFailed": 1, "ConvertError": e})

            failed = save_to_clickhouse(ck_db, df_order, 'order')
            order_df_log = pd.DataFrame([dict_log_order])
            order_df_log["DataType"] = 'order'
            order_df_log["SaveFailed"] = failed
            log_to_clickhouse(ck_db, order_df_log)

            failed = save_to_clickhouse(ck_db, df_deal, 'deal')
            deal_df_log = pd.DataFrame([dict_log_deal])
            deal_df_log["DataType"] = 'deal'
            deal_df_log["SaveFailed"] = failed
            log_to_clickhouse(ck_db, deal_df_log)
    else:
        df_data_t = pd.DataFrame()

        if not df_data.empty:
            try:
                if convert_fun.__name__ == "convert_sh_tick":
                    df_data_t = convert_fun(df_data, trading_day, dict_price["high_limit"], dict_price["low_limit"])
                else:
                    df_data_t = convert_fun(df_data, trading_day)
                invalid_data = check_invalid_data(df_data_t, dict_fun_data_type[convert_fun.__name__])
                dict_log.update({"RowNum": df_data_t.shape[0], "InvalidData": invalid_data})

            except Exception as e:
                g_logger.error(f"error in {convert_fun.__name__}, {trading_day}, {code}")
                dict_log.update({"ConvertFailed": 1, "ConvertError": e})
        
            failed = save_to_clickhouse(ck_db, df_data_t, data_type)
            df_log = pd.DataFrame([dict_log])
            df_log["DataType"] = data_type
            df_log["SaveFailed"] = failed
            log_to_clickhouse(ck_db, df_log)

#不加密的数据处理一天需要15分钟， 加密的需要30分钟。
def convert_data(trading_day: Union[pd.Timestamp, str], num_workers: int=16):
    # print(f"Start converting data for trading day: {trading_day.strftime('%Y-%m-%d')}")

    trading_day = pd.Timestamp(trading_day)

    #@bill 这里还没有倒入数据， 就不用引用datasource模块了， 因为还不存在。
    my_db = MySQLDB()
    ck_db = ClickhouseDB()

    codes_all = my_db.get_all_securities(trading_day)
    df_limit = my_db.get_limit_price(begin_date=trading_day, end_date=trading_day)
    #跳过不存在的交易日
    if df_limit is None or len(df_limit) <= 0:
        return
    df_limit = df_limit.loc[trading_day]
    
    dir_prefix = os.path.join(dir_datayes_data, trading_day.strftime("%Y/%Y.%m/%Y%m%d/%Y%m%d_"))
    # XSHE
    run_list_sz = [
        ("order", convert_sz_order, dir_prefix+"mdl_6_33_0.csv.zip"),
        ("deal", convert_sz_deal, dir_prefix+"mdl_6_36_0.csv.zip"),
        ("tick", convert_sz_tick, dir_prefix+"mdl_6_28_0.csv.zip"),
    ]
    security_id_xshe = codes_all[codes_all.str.endswith("XSHE")].map(lambda x: int(x.split(".")[0]))
    for data_type, convert_fun, dir_t in run_list_sz:
        
        try:
            df_data = read_zip_data(dir_t)
        except Exception as e:
            g_logger.error(f"read zip_data error, file_name: {dir_t}, error: {e}")
            continue

        groups = df_data.groupby("SecurityID")
        group_keys = groups.groups.keys()
        futures = []
        with ProcessPoolExecutor(num_workers,  mp_context=mp.get_context('spawn')) as pool:
            for security_id in security_id_xshe:
                df_data_t = groups.get_group(security_id).copy() if security_id in group_keys else pd.DataFrame()
                futures.append(pool.submit(convert_data_, **{'ck_db' : ck_db,
                    "trading_day": trading_day, "security_id": security_id, "df_data": df_data_t,  "data_type" :data_type,
                    "convert_fun": convert_fun, "dict_price": {}})
                    )
        [f.result() for f in futures]
        del df_data
        if data_type == "tick":
            ck_db.optimize_table(g_table_name_snap, partition=trading_day)
        elif data_type == "order":
            ck_db.optimize_table(g_table_name_order, partition=trading_day)
        elif data_type == "deal":
            ck_db.optimize_table(g_table_name_deal, partition=trading_day)
        else:
            ck_db.optimize_table(g_table_name_order, partition=trading_day)
            ck_db.optimize_table(g_table_name_deal, partition=trading_day)
            ck_db.optimize_table(g_table_name_snap, partition=trading_day)
        gc.collect()
    ck_db.optimize_table(g_table_name_convert_info)

    # XSHG
    if trading_day <= pd.Timestamp("20210620"):
        dir_kuanrui_order = os.path.join(dir_kuanrui_data, trading_day.strftime("%Y/%Y.%m/%Y%m%d/Entrust_new_SH_%Y%m%d.7z"))        
        run_list_sh = [
            ("order", convert_sh_order_hist, dir_kuanrui_order, "kuanrui"),
            ("deal", convert_sh_deal_old, dir_prefix+"Transaction.csv.zip", "datayes"),
            ("tick", convert_sh_tick, dir_prefix+"MarketData.csv.zip", "datayes")
        ]
    elif trading_day <= pd.Timestamp("20231221"):
        run_list_sh = [
            ("order", convert_sh_order_old, dir_prefix+"mdl_4_19_0.csv.zip", "datayes"),
            ("deal", convert_sh_deal_old, dir_prefix+"Transaction.csv.zip", "datayes"),
            ("tick", convert_sh_tick, dir_prefix+"MarketData.csv.zip", "datayes")
        ]
    else:
        run_list_sh = [
            ("order_deal", convert_sh_order_deal, dir_prefix+"mdl_4_24_0.csv.zip", "datayes"),
            ("tick", convert_sh_tick, dir_prefix+"MarketData.csv.zip", "datayes")
        ]
    
    security_id_xshg = codes_all[codes_all.str.endswith("XSHG")].map(lambda x: int(x.split(".")[0]))
    for data_type, convert_fun, dir_t, data_source in run_list_sh:
        
        try:
            df_data = read_zip_data(dir_t)
        except Exception as e:
            g_logger.error(f"read zip_data error, file_name: {dir_t}, error: {e}")
            continue
        
        groups = df_data.groupby("SecurityID")
        group_keys = groups.groups.keys()
        futures = []
        with ProcessPoolExecutor(num_workers,  mp_context=mp.get_context('spawn')) as pool:
            for security_id in security_id_xshg:
                dict_price = {}
                if data_type == "tick":
                    code = str(security_id).zfill(6) + ".XSHG"
                    dict_price["high_limit"] = df_limit.loc[code, "high_limit"] if code in df_limit.index else 0.0
                    dict_price["low_limit"] = df_limit.loc[code, "low_limit"] if code in df_limit.index else 0.0
                
                df_data_t = groups.get_group(security_id).copy() if security_id in group_keys else pd.DataFrame()
                futures.append(pool.submit(convert_data_, **{'ck_db' : ck_db,
                    "trading_day": trading_day, "security_id": security_id, "df_data": df_data_t, "data_type" :data_type,
                    "convert_fun": convert_fun, "dict_price": dict_price})
                    )
            
        [f.result() for f in futures]

        del df_data
        if data_type == "tick":
            ck_db.optimize_table(g_table_name_snap, partition=trading_day)
        elif data_type == "order":
            ck_db.optimize_table(g_table_name_order, partition=trading_day)
        elif data_type == "deal":
            ck_db.optimize_table(g_table_name_deal, partition=trading_day)
        else:
            ck_db.optimize_table(g_table_name_order, partition=trading_day)
            ck_db.optimize_table(g_table_name_deal, partition=trading_day)
            ck_db.optimize_table(g_table_name_snap, partition=trading_day)
        gc.collect()
    ck_db.optimize_table(g_table_name_convert_info)
    g_logger.info(f"Finished converting data for trading day: {trading_day.strftime('%Y-%m-%d')}")
    return None


def convert_m_order(df_order: pd.DataFrame, trading_day: pd.Timestamp):
    
    dict_rename = {
        "SecuCode": "Code",
        "OrderTime": "Time",
        }
    
    df_order = df_order.rename(columns=dict_rename)

    df_order["TradingDay"] = trading_day.date()

    code = str(df_order["Code"].values[0]).zfill(6)
    code += {"0": ".XSHE", "3": ".XSHE", "6": ".XSHG"}[code[0]]
    df_order["Code"] = code
    df_order["Time"] = df_order["Time"].astype(str).str.zfill(9)
    df_order["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_order["Time"].str[:6] + "." + df_order["Time"].str[-3:], format="%Y-%m-%d %H%M%S.%f")
    df_order["UpdateTime"] = df_order["Time"]
    df_order["Side"] = np.int16(0)
    df_order["OrderType"] = df_order["OrderType"].astype("int16")
    if code.endswith("XSHE"):
        df_order.loc[df_order["OrderType"].isin([11, 12, 13]), "Side"] = 1
        df_order["OrderType"] = df_order["OrderType"].map({1:1, 2:2, 3:3, 11:1, 12:2, 13:3}).astype("int16")
        df_order["SeqNum"] = df_order["OrderID"]
    else:
        df_order.loc[df_order["OrderType"].isin([10, -11]), "Side"] = 1
        df_order["OrderType"] = df_order["OrderType"].map({0: 2, 10: 2, -1: 5, -11: 5}).astype("int16")
        df_order["SeqNum"] = df_order["BizIndex"]
    
    df_order["Price"] = (df_order["Price"].astype("float64") / 100).round(2)
    df_order["Volume"] = df_order["Volume"].astype("float64")
    # astype
    fields_int64 = ["OrderID", "Channel", "SeqNum"]
    df_order[fields_int64] = df_order[fields_int64].astype("int64")

    df_order = df_order[ORDER_COLUMNS].sort_values(by="SeqNum").reset_index(drop=True)
    
    return df_order


def convert_m_deal(df_deal: pd.DataFrame, trading_day: pd.Timestamp):
    
    dict_rename = {
        "SecuCode": "Code",
        "DealTime": "Time",
        "SellID": "SaleOrderID",
        "BuyID": "BuyOrderID",
        "DealID": "SeqNum"
        }
    
    df_deal = df_deal.rename(columns=dict_rename)
    df_deal["TradingDay"] = trading_day.date()
    code = str(df_deal["Code"].values[0]).zfill(6)
    df_deal["Code"] = code + {"0": ".XSHE", "3": ".XSHE", "6": ".XSHG"}[code[0]]
    df_deal["Time"] = df_deal["Time"].astype(str).str.zfill(9)
    df_deal["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_deal["Time"].str[:6] + "." + df_deal["Time"].str[-3:], format="%Y-%m-%d %H%M%S.%f")
    df_deal["UpdateTime"] = df_deal["Time"]
    df_deal["Side"] = df_deal["Side"].map({0: 0, 1: 1, -1: 4, -11: 4}).astype('int16')
    df_deal["Price"] = (df_deal["Price"].astype("float64") / 100).round(2)
    df_deal["Volume"] = df_deal["Volume"].astype("float64")
    df_deal["Money"] = df_deal["Price"] * df_deal["Volume"]
    # astype
    fields_int64 = ["SaleOrderID", "BuyOrderID", "Channel", "SeqNum"]
    df_deal[fields_int64] = df_deal[fields_int64].astype("int64")

    df_deal = df_deal[DEAL_COLUMNS].sort_values(by='SeqNum').reset_index(drop=True)
    
    return df_deal


def convert_m_tick(df_tick: pd.DataFrame, trading_day: pd.Timestamp, high_limit: float = 0.0, low_limit: float =0.0, pre_close: float =0.0):
    
    dict_rename = {
        "SecuCode": "Code",
        "TickTime": "Time",
        "WeightBidPrice": "AvgBidPrice",
        "WeightAskPrice": "AvgAskPrice",
        "Price": "CurrentPrice",
        "TotalDealNum": "TradeNum",
        "TotalTurnover": "TotalMoney",

        "AskOrder1": "AskNum1", "AskOrder2": "AskNum2", "AskOrder3": "AskNum3", "AskOrder4": "AskNum4", "AskOrder5": "AskNum5", 
        "AskOrder6": "AskNum6", "AskOrder7": "AskNum7", "AskOrder8": "AskNum8", "AskOrder9": "AskNum9", "AskOrder10": "AskNum10", 
        "BidOrder1": "BidNum1", "BidOrder2": "BidNum2", "BidOrder3": "BidNum3", "BidOrder4": "BidNum4", "BidOrder5": "BidNum5", 
        "BidOrder6": "BidNum6", "BidOrder7": "BidNum7", "BidOrder8": "BidNum8", "BidOrder9": "BidNum9", "BidOrder10": "BidNum10", 
        }
    
    df_tick = df_tick.rename(columns=dict_rename)
    columns_price = [f"{i}Price{j}" for i in ["Ask", "Bid"] for j in range(1, 11)] + ["CurrentPrice"]
    df_tick[columns_price] = (df_tick[columns_price].astype("float64") / 100).round(2) # 仅适用于股票，可转债是小数点后三位
    df_tick["TradingDay"] = trading_day.date()
    code = str(df_tick["Code"].values[0]).zfill(6)
    df_tick["Code"] = code + {"0": ".XSHE", "3": ".XSHE", "6": ".XSHG"}[code[0]]
    df_tick["Time"] = df_tick["Time"].astype(str).str.zfill(9)
    df_tick["Time"] = pd.to_datetime(trading_day.strftime("%Y-%m-%d ") + df_tick["Time"].str[:6] + "." + df_tick["Time"].str[-3:], format="%Y-%m-%d %H%M%S.%f")
    df_tick["UpdateTime"] = df_tick["Time"]
    df_tick["HighLimitPrice"] = high_limit
    df_tick["LowLimitPrice"] = low_limit
    df_tick["PreClosePrice"] = pre_close

    # notice
    df_tick["IOPV"] = np.float64(0.0)
    df_tick = df_tick.sort_values(by=["Time", "TotalVolume"]).reset_index(drop=True)
    df_tick["HighestPrice"] = df_tick["CurrentPrice"].cummax()
    df_tick["LowestPrice"] = df_tick["CurrentPrice"].replace(0.0, np.nan).cummin().replace(np.nan, 0.0)
    arr_price = df_tick[df_tick["CurrentPrice"] > 0.0]["CurrentPrice"].values
    df_tick["OpenPrice"] = arr_price[0] if len(arr_price) > 0 else 0.0
    df_tick["Channel"] = np.int64(0)
    df_tick["SeqNum"] = np.arange(df_tick.shape[0], dtype=np.int64)
    
    # astype
    columns_float64 = pd.Index(TICK_COLUMNS).difference(["TradingDay", "Code", "Time", "UpdateTime", "Channel", "SeqNum"])
    df_tick[columns_float64] = df_tick[columns_float64].astype("float64").fillna(0.0) # fillna(0.0): jq的处理方法
    
    df_tick = df_tick[TICK_COLUMNS].reset_index(drop=True)

    return df_tick


def convert_mdata_(ck_db : ClickhouseDB, trading_month: str, code: str, trading_days: pd.Index, data_type: str, df_price: pd.DataFrame):

    assert data_type in ["order", "deal", "tick"]
    df_price = df_price.reindex(trading_days).fillna(0.0)

    dict_convert_fun = {
        "order": convert_m_order,
        "deal": convert_m_deal,
        "tick": convert_m_tick,
    }

    year_, month_ = map(int, trading_month.split("."))
    next_trading_month = f"{year_}.{str(month_ + 1).zfill(2)}" if month_ != 12 else f"{year_+1}.{str(1).zfill(2)}"
    file_name = f"{trading_month}.01_{next_trading_month}.01".replace(".", "") + ".zip"
    dict_name = {"order": "逐笔委托", "deal": "逐笔成交", "tick": "快照"}
    zip_dir = os.path.join(dir_mdatayes_data, dict_name[data_type], trading_month.split(".")[0], file_name)
    
    try:
        df_data = read_zip_data(zip_dir, code=code)
    except Exception as e:
        g_logger.warning(f"read zip_data error, file_name: {zip_dir}, code: {code}, error: {e}")
        return None
    
    list_data = []
    list_log = []
    dict_log = {"Time": pd.Timestamp.now(), "Code": code, "DataType": data_type, "RowNum": 0, 
                "ConvertFailed": 0, "ConvertError": "", "InvalidData": 0}
    for trading_day in trading_days:
        df_data_t = df_data.loc[df_data["TradingDay"] == int(trading_day.strftime("%Y%m%d"))].copy()
        dict_log_t = dict_log.copy()
        dict_log_t.update({"TradingDay": trading_day.date()})
        df_data_t2 = pd.DataFrame()
        if not df_data_t.empty:
            try:
                if data_type == "tick":
                    ser_price = df_price.loc[trading_day].copy()
                    df_data_t2 = dict_convert_fun["tick"](df_data_t, trading_day, ser_price.high_limit, ser_price.low_limit, ser_price.pre_close)
                else:
                    df_data_t2 = dict_convert_fun[data_type](df_data_t, trading_day)
                invalid = check_invalid_data(df_data_t2, data_type)
                dict_log_t.update({"RowNum": df_data_t2.shape[0], "InvalidData": invalid})
            except Exception as e:
                g_logger.error(f"error in {dict_convert_fun[data_type].__name__}, {trading_day}, {code}")
                dict_log_t["ConvertFailed"] = 1
                dict_log_t["ConvertError"] = e
        
        list_data.append(df_data_t2)
        list_log.append(dict_log_t)

    df_data = pd.concat(list_data)
    if not df_data.empty:
        failed = save_to_clickhouse(ck_db, df_data, data_type)
    else:
        failed = 0
    df_log = pd.DataFrame(list_log)
    df_log["SaveFailed"] = failed
    log_to_clickhouse(ck_db, df_log)    
    return None

def convert_mdata(trading_month: str, num_workers: int=16):

    try:
        datetime.datetime.strptime(trading_month, "%Y.%m")
    except:
        raise ValueError(f"illegal trading_month: {trading_month}")
    
    if trading_month < "2021.07":
        g_logger.error("2021.07之前的数据需要使用其他数据源，mdata没有历史的逐笔委托数据")
        return None

    my_db = MySQLDB()
    ck_db = ClickhouseDB()
    
    trading_days = my_db.get_all_trade_days()
    trading_days = trading_days[trading_days.map(lambda x: x.strftime("%Y.%m") == trading_month)].sort_values()
    all_codes = my_db.get_period_all_securities(trading_days[0], trading_days[-1])
    dict_codes = {trading_day: my_db.get_all_securities(trading_day) for trading_day in trading_days}
    
    df_limit = my_db.get_limit_price(begin_date=trading_days[0], end_date=trading_days[-1]).swaplevel().sort_index()
    df_price = my_db.get_daily_price(begin_date=trading_days[0], end_date=trading_days[-1], tuple_fields=("pre_close", )).swaplevel().sort_index()
    df_price = pd.concat([df_price, df_limit], axis=1)

    with ProcessPoolExecutor(num_workers,  mp_context=mp.get_context('spawn')) as pool:
        for data_type in ["order", "deal", "tick"]:
            futures = []
            for code in all_codes:
                trading_days_t = [trading_day for trading_day in trading_days if code in dict_codes[trading_day]]
                df_price_t = df_price.loc[code].copy() if code in df_price.index.get_level_values(0).unique() and data_type == "tick" else pd.DataFrame()
                futures.append(pool.submit(convert_mdata_, **{'ck_db':ck_db,
                    "trading_month": trading_month, "code": code, "trading_days": trading_days_t, "data_type": data_type, "df_price": df_price_t})
                    )
            [f.result() for f in futures]
            gc.collect()
            #手动合并parts
            # if data_type == "tick":
            #     ck_db.optimize_table(g_table_name_snap)
            # elif data_type == "order":
            #     ck_db.optimize_table(g_table_name_order)
            # elif data_type == "deal":
            #     ck_db.optimize_table(g_table_name_deal)
    # ck_db.optimize_table(g_table_name_snap)
    # ck_db.optimize_table(g_table_name_order)
    # ck_db.optimize_table(g_table_name_deal)
    # ck_db.optimize_table(g_table_name_convert_info)
    return None

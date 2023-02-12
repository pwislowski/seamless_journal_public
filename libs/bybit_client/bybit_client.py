import pandas as pd
import numpy as np
from pybit import usdt_perpetual, inverse_perpetual
from datetime import datetime as dt


# redesign the `bybit client`
class Bybit_client:

    def __init__(self, api_key, api_secret, contract_type = "inverse") -> None:
        self.api_key = api_key,
        self.api_secret = api_secret
        self.contract_type = contract_type
        endpoint = "https://api.bybit.com"

        if contract_type == "inverse":
            self.cli = inverse_perpetual.HTTP(
                endpoint = endpoint,
                api_key= api_key,
                api_secret = api_secret
            )
        
        elif contract_type == "usdt_perp":
            self.cli = usdt_perpetual.HTTP(
                endpoint = endpoint,
                api_key= api_key,
                api_secret = api_secret
            )
        
        else:
            raise TypeError("Unsupported contract type -- select ['inverse', 'usdt_perp']")
    
    def current_position(self, symbol = None):
        
        if symbol is None:
            d = self.cli.my_position()["result"]
        
        else:
            d = self.cli.my_position(
                symbol = symbol
            )

        dic = {}

        for e in d:
            obj = e["data"]
            
            try:
                if obj["position_value"] > "0":
                    dic[obj["symbol"]] = obj 
            
            except TypeError:
                if obj["position_value"] > 0:
                    dic[obj["symbol"]] = obj 

        return dic
    
    def closed_pnl(self, symbols = None, c_pos = False, as_df = False):
        
        def cleaner(raw):
            dic = {}

            for e in raw:

                for k, v in e.items():
                    if k in dic.keys():
                        dic[k].append(v)
                
                    else:
                        dic[k] = []
                        dic[k].append(v)
            
            return pd.DataFrame(dic)
        
        if type(symbols) is str:
            pnl = self.cli.closed_profit_and_loss(
                symbol = symbols
            )
            pnl = pnl["result"]["data"]

            return cleaner(pnl)
        
        elif type(symbols) is list:
            
            dic_main = {}

            for a in symbols:
                
                pnl = self.cli.closed_profit_and_loss(
                    symbol = a
                )
                pnl = pnl["result"]["data"]

                dic_main[a] = cleaner(pnl)
            
            return dic_main
        
        elif c_pos is True:
            
            dic_main = {}

            for a in self.current_position().keys():
                
                pnl = self.cli.closed_profit_and_loss(
                    symbol = a
                )
                pnl = pnl["result"]["data"]

                dic_main[a] = cleaner(pnl)
            
            if as_df is True:
                return pd.concat([df for df in dic_main.values()], ignore_index= True)
            
            else:
                return dic_main

        else:
            raise TypeError("Unsupported data type -- supported ['str', 'list']")
        
    def trade_records(self, symbol, exec_type = "Trade"):

        def w_avg(x_df):
            d = x_df["exec_price"]
            w = x_df["exec_value"]

            return (d * w).sum() / w.sum()

        def tmstmp_process(x_df):

            if self.contract_type == "usdt_perp":
                arr = x_df["trade_time"]

                x_df["exec_date_utc"] = [dt.fromtimestamp(r).date() for r in arr]
                x_df["exec_time_utc"] = [dt.fromtimestamp(r).time() for r in arr]


                pass

            else:
                arr = x_df["exec_time"]

                x_df["exec_date_utc"] = [dt.fromtimestamp(r).date() for r in arr]
                x_df["exec_time_utc"] = [dt.fromtimestamp(r).time() for r in arr]

            return x_df

        page = 1
        j = self.cli.user_trade_records(
            symbol = symbol,
            page = page)
        
        if self.contract_type == "usdt_perp":
            j = j["result"]["data"]


        else:
        
            j = j["result"]["trade_list"]

        dic = {}

        while j is not None:
            for e in j:
                for k,v in e.items():

                    try:
                        v = float(v)
                    except (TypeError, ValueError):
                        v = v

                    if k in dic.keys():
                        dic[k].append(v)
                    else:
                        dic[k] = []
                        dic[k].append(v)
            
            page += 1
            j = self.cli.user_trade_records(
            symbol = symbol,
            page = page)

            if self.contract_type == "usdt_perp":
                j = j["result"]["data"]


            else:
                j = j["result"]["trade_list"]

        df = pd.DataFrame(dic)

        # group by internal values
        if self.contract_type == "usdt_perp":
            df = (
                df[
                    (df["exec_type"] == "Trade") &
                    (df["closed_size"] == 0)
                ].sort_values(
                    by = "trade_time",
                    ascending= False,
                    ignore_index = True
                ).pipe(
                    tmstmp_process
                )
            )
            return df

        else:    
            l = (
                df.query(F"exec_type == '{exec_type}'")
                .sort_values(
                    by = "exec_time",
                    ascending = False,
                    ignore_index= True
                )
                .groupby(
                    by = ["symbol", "cross_seq", "side"],
                    sort = False
                ).agg(
                    {
                        # 'exec_price' : np.mean, use weighted mean
                        'closed_size' : np.sum,
                        'exec_fee' : np.sum,
                        'exec_qty' : np.sum,
                        'exec_time' : np.min,
                        'exec_value' : np.sum
                    }
                ).pipe(
                    tmstmp_process
                )
            )

            r = (
                df.query(F"exec_type == '{exec_type}'")
                .sort_values(
                    by = "exec_time",
                    ascending = False,
                    ignore_index= True
                )
                .groupby(
                    by = ["symbol", "cross_seq", "side"],
                    sort = False
                ).apply(
                    w_avg
                )
            )
            r.name = "exec_price"

            return l.join(r, on = ["symbol", "cross_seq", "side"])
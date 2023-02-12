import http.client
import json, time, hmac, hashlib, base64
import pandas as pd
import numpy as np
from datetime import datetime


class CoinbaseAPI:
    def __init__(
        self, api_key: str, api_secret: str, URL: str = "api.coinbase.com"
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.cc = http.client.HTTPSConnection(URL)
        self.URL = URL

        pass

    @property
    def _account_id(self) -> str:
        ENDPOINT = "/api/v3/brokerage/accounts"
        headers = self._get_auth_headers("GET", ENDPOINT)

        cc = self.cc
        cc.request("GET", ENDPOINT, headers=headers)

        res = cc.getresponse()
        output = res.read().decode("utf-8")
        output = json.loads(output)

        account_id = output["accounts"][0]["uuid"]

        return account_id

    def _get_auth_headers(self, method, ENDPOINT) -> dict:
        """Returns auth headers"""
        timestamp = str(int(time.time()))
        secret = self.api_secret.encode()
        message = timestamp + method + ENDPOINT
        message = message.encode()

        signature = hmac.new(secret, message, hashlib.sha256).hexdigest()

        return {
            "Content-Type": "Application/JSON",
            "CB-ACCESS-SIGN": signature,
            "CB-ACCESS-TIMESTAMP": timestamp,
            "CB-ACCESS-KEY": self.api_key,
        }

    @property
    def transactions_wallet(self) -> json:
        """Retrieve transactions for an account or accounts. Provide `account_id` with <account_id>."""

        cc = self.cc
        ENDPOINT = f"/api/v3/brokerage/orders/historical/fills"
        headers = self._get_auth_headers("GET", ENDPOINT)

        cc.request("GET", ENDPOINT, headers=headers)

        res = cc.getresponse()
        output = res.read().decode("utf-8")

        fills = json.loads(output)["fills"]

        data = dict()
        for obj in fills:
            for k, v in obj.items():  # obj is dict
                if k in data.keys():
                    data[k].append(v)
                else:
                    data[k] = list()
                    data[k].append(v)

        return data
    
    def transactions_inpandas(self, side:str = "BUY") -> pd.DataFrame:
        """Returns `pd.DataFrame` of all transactions, by default on the Buy-side,"""

        def timestamp_decon(x_df):
            _fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
            timestamps = x_df['sequence_timestamp']
            processed = timestamps.map(lambda x: datetime.strptime(x, _fmt))

            x_df['date'] = [D.date() for D in processed]
            x_df['time'] = [D.time() for D in processed]

            return x_df

        data = self.transactions_wallet
        df = pd.DataFrame(data)

        df.commission = df.commission.astype(float)
        df.size = df.size.astype(float)
        df.price = df.price.astype(float)
        df.commission = df.commission.astype(float)

        df = (df
            .pipe(
                timestamp_decon
            ).groupby(
                by = [
                    'order_id',
                    'product_id',
                    'side',
                    'date'
                ]
            ).agg(
                {
                    'price' : np.mean,
                    'time' : np.min,
                    'size' : np.sum, #TODO: seems to be off
                    'commission' : np.sum,
                    'sequence_timestamp' : np.min
                }
            ).reset_index()
            .sort_values(
                by = 'sequence_timestamp',
                ascending= False,
                ignore_index = True
            ).query(
                f'side == "{side}"' #because buying spot
            )
        )

        

        return df


if __name__ == "__main__":
    import pandas as pd

    cli = CoinbaseAPI('api_key', 'api_secret')

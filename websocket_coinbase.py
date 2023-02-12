from dotenv import load_dotenv
from typing import NamedTuple
import os

from libs.wrapper_coinbase.coinbase import CoinbaseAPI
from libs.notiondb.notiondb import NotionClient, Number, Select, Title, Date
from libs.trade_logger import trade_logger

def load_creds(path:str = "./auth/cb.env") -> NamedTuple:
    """Load API Credentials.
    :::path::: path to `.env` """

    load_dotenv(path)
    
    class Creds(NamedTuple):
        api_key: str
        api_secret: str
    
    return Creds(
        os.environ.get('api_key'),
        os.environ.get('api_secret')
    )

def get_latest_transactions(api_key:str, api_secret:str, client:CoinbaseAPI = CoinbaseAPI) -> dict:
    """Returns latest transaction for all assests"""

    client = client(api_key, api_secret)

    df = client.transactions_inpandas()

    assets_traded = df['product_id'].unique().tolist()
    latest_trades = dict()
    for asset in assets_traded:
        _res = df.query(f'product_id == "{asset}"').reset_index()
        _res = _res.iloc[0].to_dict()

        #transform datetime into str
        _res['date'] = _res['date'].strftime('%Y-%m-%d')
        _res['time'] = _res['time'].strftime('%H:%M-%S')

        latest_trades[asset] = _res
    
    return latest_trades

@trade_logger.logger
def log_trade(d: dict, **kwarg) -> dict:
    """Logs and returns a dict."""
    return d

def send_to_notion(transformed_input, raw_dic=None):
    """Transform dictionary output into notion input and send"""
    load_dotenv("./auth/notion.env")

    notion_creds = dict(
        api = os.environ.get("api"),
        db = os.environ.get("db_records")
    )

    cli_notion = NotionClient(
        api=notion_creds["api"],
        idDb=notion_creds["db"]
    )
    cli_notion.create_page(transformed_input, raw_dic)


def transform_input(
    trade_info: dict, market_type="Cryptocurrency", exchange="Bybit"
) -> dict:
    """Tranforms `coinbase` query into `notion` query"""

    # query inputs
    _title = Title("trade").wrapper()
    _asset = Select("Asset").wrapper(trade_info["product_id"])
    _entryprice = Number("Entry Price").wrapper(trade_info["price"])
    _entrydate = Date("Entry Date").wrapper(
        date=trade_info["date"],
        time=trade_info["time"]
    )
    _side = Select("Side").wrapper("Long" if trade_info["side"] == "BUY" else "Short")
    _market = Select("Market").wrapper(market_type)
    _exchange = Select("Exchange").wrapper(exchange)

    # output
    query = {
        **_title,
        **_asset,
        **_entryprice,
        **_entrydate,
        **_side,
        **_market,
        **_exchange,
    }
    return query


def main():
    creds = load_creds()
    transactions = get_latest_transactions(
        creds.api_key,
        creds.api_secret
    )

    for name, val in transactions.items(): #iterate over dictionaries
        check = trade_logger.trade_comparer(val, name)

        if check: #records match log, do nothing
            continue
        else: #records diverge, update notion
            trade = log_trade(val, **{'fname' : f'CB_{name}'})
            notion_query = transform_input(trade, exchange = "Coinbase")
            send_to_notion(
                notion_query,
                trade
            )

if __name__ == "__main__":
    from time import sleep

    while True:
        interval = 5 * 60 # 5 minutes

        main()
        sleep(interval)


            


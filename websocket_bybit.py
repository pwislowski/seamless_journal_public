from libs.bybit_client.bybit_client import Bybit_client
from libs.notiondb.notiondb import NotionClient
from libs.trade_logger.trade_logger import logger, trade_comparer
from datetime import datetime as dt

from typing import NamedTuple, Any
from time import sleep

from dotenv import load_dotenv
from os import environ as os_creds


def load_creds(path: str = "./auth/.env") -> NamedTuple:

    load_dotenv(path)

    class Creds(NamedTuple):
        api_key: str
        api_secret: str

    Creds.api_key = os_creds.get("api_key")
    Creds.api_secret = os_creds.get("api_secret")

    return Creds


def open_position_inverse(creds) -> Any:
    """check if in position for the `inverse`"""
    client = Bybit_client(
        api_key=creds.api_key, api_secret=creds.api_secret, contract_type="inverse"
    )

    poistions = client.current_position()

    class Client(NamedTuple):
        cli: object
        assets: list

    Client.cli = client
    Client.assets = list(poistions.keys())

    return Client


def open_position_perpusdt(creds) -> Any:
    """check if in position for the `inverse`"""
    client = Bybit_client(
        api_key=creds.api_key, api_secret=creds.api_secret, contract_type="usdt_perp"
    )

    poistions = client.current_position()

    class Client(NamedTuple):
        cli: object
        assets: list

    Client.cli = client
    Client.assets = list(poistions.keys())

    return Client


def recent_opened_position(client: Bybit_client, symbol: str, **kwargs) -> Any:
    """Get recent opened positions that will be sent to `notion db`"""

    recent_trade = (
        client.trade_records(symbol=symbol)
        .reset_index(drop=False)
        .query("closed_size == 0")
        .iloc[0]
    ).to_dict()

    # remove datetime obj to enable `eval` method from `logger.py`
    recent_trade["exec_date_utc"] = recent_trade["exec_date_utc"].strftime("%Y-%m-%d")
    recent_trade["exec_time_utc"] = recent_trade["exec_time_utc"].strftime("%H:%M:%S")

    return recent_trade


@logger
def log_trade(client: Bybit_client, symbol: str, fname: str):
    return recent_opened_position(client, symbol, **{"fname": fname})


def is_in_position(creds: NamedTuple) -> Any:
    """Check if an account is currently in a position.
    - Yes: check if `trade_id` in `log`
    - No: do nothing"""
    clients = [open_position_inverse(creds), open_position_perpusdt(creds)]

    if all([c.assets == None for c in clients]):
        return None

    else:
        lst_trade_infos = []
        for c_pack in [c for c in clients if c.assets != None]:
            asset = c_pack.assets
            cli = c_pack.cli

            # iterate through symbols to get a recent opened position
            for a in asset:

                fname = f"BYBIT_{a}"
                trade_info = recent_opened_position(client=cli, symbol=a)

                if trade_comparer(trade_info, fname):
                    # already logged
                    lst_trade_infos.append(None)

                else:

                    trade_info = log_trade(client=cli, symbol=a, fname=f"BYBIT_{a}")

                    lst_trade_infos.append(trade_info)

        return lst_trade_infos


def send_to_notion(transformed_input, raw_dic=None):
    """Transform dictionary output into notion input and send"""
    load_dotenv("./auth/notion.env")
    notion_creds = dict(api=os_creds.get("api"), db=os_creds.get("db_records"))

    cli_notion = NotionClient(api=notion_creds["api"], idDb=notion_creds["db"])

    cli_notion.create_page(transformed_input, raw_dic)


def transform_input(
    trade_info: dict, market_type="Cryptocurrency", exchange="Bybit"
) -> dict:
    """Tranforms `bybit` query into `notion` query"""
    from libs.notiondb.notiondb import Number, Select, Date, Title

    # query inputs
    _title = Title("trade").wrapper()
    _asset = Select("Asset").wrapper(trade_info["symbol"])
    _entryprice = Number("Entry Price").wrapper(trade_info["exec_price"])
    _entrydate = Date("Entry Date").wrapper(
        date=trade_info["exec_date_utc"], time=trade_info["exec_time_utc"]
    )
    _side = Select("Side").wrapper("Long" if trade_info["side"] == "Buy" else "Short")
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
    creds = load_creds(path="./auth/bybit.env")
    obj = is_in_position(creds)

    if obj is None:
        # no request to update notion db
        pass

    else:
        for trade_info in obj:
            if trade_info is not None:
                # send notion request
                transformed_input = transform_input(trade_info)
                send_to_notion(transformed_input, raw_dic=trade_info)


if __name__ == "__main__":

    while True:
        sleep_time = 60 * 5  # run every 5 minutes

        main()
        sleep(sleep_time)

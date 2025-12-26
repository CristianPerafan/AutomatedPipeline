import logging
import requests
import time
import json

import polars as pl

from typing import Dict, List
from pathlib import Path


from etl.config import Config
from utils.http import create_http_session


RAW_DATA_PATH = Path(Config.DATA_DIR) / "raw" / "coins_data.json"
RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("etl.extract")

def fetch_coin(
    session: requests.Session,
    coin: str,
    days_back: int,
) -> Dict:
    base_url = "https://api.coingecko.com/api/v3/coins"
    endpoint = f"{base_url}/{coin}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days_back,
    }

    try:
        response = session.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched data for {coin}")
        return {
            "coin": coin,
            "data": data,
        }
    except requests.RequestException as e:
        logger.error(f"Error fetching data for {coin}: {e}")
        return {}
    
def fetch_coins_data(
    coins: list,
    days_back: int,
    sleep_seconds: int = 2,
) ->  List[Dict]:
    if not coins:
        raise ValueError("The coins list is empty.")
    
    session = create_http_session()
    coins_data = []

    for coin in coins:
        try:
            data = fetch_coin(session, coin, days_back)
            if data:
                coins_data.append(data)
                logger.info(f"Successfully fetched data for {coin}")
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {coin}: {e}")

        time.sleep(sleep_seconds)

    return coins_data

def run():
    logger.info("Starting data extraction process")

    assets_data = fetch_coins_data(
        coins=Config.COIN_LIST,
        days_back=Config.DAYS_BACK,
    )

    with open(RAW_DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(assets_data, f, ensure_ascii=False)   

    logger.info(f"Saved raw data to {RAW_DATA_PATH}")
    logger.info("Data extraction process completed")

if __name__ == "__main__":
    run()
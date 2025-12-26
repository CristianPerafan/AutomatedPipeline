import logging
import requests
import time
import json

import polars as pl

from typing import Dict, List
from pathlib import Path


from etl.config import Config
from utils.http import create_http_session

logger = logging.getLogger("etl.extract")


def fetch_coin(
    session: requests.Session,
    symbol: str,
    days_back: int,
) -> Dict:
    base_url = "https://api.coingecko.com/api/v3/coins"
    endpoint = f"{base_url}/{symbol}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days_back,
    }

    try:
        response = session.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched data for {symbol}")
        return {
            "symbol": symbol,
            "data": data,
        }
    except requests.RequestException as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return {}
    
def fetch_coins_data(
    symbols: list,
    days_back: int,
    sleep_seconds: int = 2,
) ->  List[Dict]:
    if not symbols:
        raise ValueError("The symbols list is empty.")
    
    session = create_http_session()
    coins_data = []

    for symbol in symbols:
        try:
            data = fetch_coin(session, symbol, days_back)
            if data:
                coins_data.append(data)
                logger.info(f"Successfully fetched data for {symbol}")
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {symbol}: {e}")

        time.sleep(sleep_seconds)

    
            
        
    

def extract():
    logger.info("Starting data extraction process")

    assets_data = fetch_coins_data(
        symbols=Config.COIN_LIST,
        days_back=Config.DAYS_BACK,
    )

    RAW_DATA_PATH = Path(Config.DATA_DIR) / "raw" / "coins_data.json"
    RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RAW_DATA_PATH, "w") as f:
        json.dump(assets_data, f)
        
    logger.info(f"Saved raw data to {RAW_DATA_PATH}")
    logger.info("Data extraction process completed")







    

        


if __name__ == "__main__":
    extract()
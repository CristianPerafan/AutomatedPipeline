import logging
import requests
import time

from etl.config import Config

logger = logging.getLogger("etl.extract")

def fetch_assets():

    assets_list = Config.ASSETS_LIST
    days_back = Config.DAYS_BACK

    for asset in assets_list:
        logger.info(f"Fetching data for asset: {asset} for the past {days_back} days")
        url = f"https://api.coingecko.com/api/v3/coins/{asset}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": days_back,
            "interval": "daily"
        }

        print(f"Requesting URL: {url} with params: {params}")

        r = requests.get(url, params=params, timeout=100)
        if r.status_code == 200:
            data = r.json()
            logger.info(f"Successfully fetched data for {asset}")
        else:
            logger.error(
                f"Failed to fetch data for {asset}. Status code: {r.status_code}"
            )

        time.sleep(1)

        


if __name__ == "__main__":
    fetch_assets()
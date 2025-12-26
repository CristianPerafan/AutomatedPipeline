import json
import logging
import polars as pl

from pathlib import Path
from typing import List, Dict

from etl.config import Config

RAW_DATA_PATH = Path(Config.DATA_DIR) / "raw" / "coins_data.json"
PROCESSED_DATA_PATH = Path(Config.DATA_DIR) / "processed" / "coins_data.parquet"

logger = logging.getLogger("etl.transform")

def _to_df(
    values: List[List],
    asset: str,
    column_name: str
) -> pl.DataFrame:
    """
    Convierte listas [timestamp, value] en Polars DataFrame
    """
    return pl.DataFrame(
        values,
        schema=["timestamp", column_name],
        orient="row"
    ).with_columns(
        [
            pl.from_epoch("timestamp", time_unit="ms"),
            pl.lit(asset).alias("asset"),
        ]
    )


def transform(raw_data: List[Dict]) -> pl.DataFrame:
    frames = []

    for coin_data in raw_data:
        coin = coin_data["coin"]
        data = coin_data["data"]

        price_df = _to_df(data["prices"], coin, "price_usd")
        volume_df = _to_df(data["total_volumes"], coin, "volume_usd")
        market_cap_df = _to_df(data["market_caps"], coin, "market_cap_usd")

        df = (
            price_df
            .join(volume_df, on=["timestamp", "asset"])
            .join(market_cap_df, on=["timestamp", "asset"])
        )

        frames.append(df)

    return pl.concat(frames)


def run():
    logger.info("Starting data transformation process")


    with open(RAW_DATA_PATH, "r") as f:
        raw_data = json.load(f)

    logger.info(f"Loading raw data from {RAW_DATA_PATH}")

    df = transform(raw_data)

    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(PROCESSED_DATA_PATH)


    logger.info("Data transformation process completed")



if __name__ == "__main__":
    run()
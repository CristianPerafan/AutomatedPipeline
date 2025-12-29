import json
import logging
import polars as pl
import polars_talib as plta

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

def _ohlc_to_df(
    ohlc_data: list[list],
    asset: str,
) -> pl.DataFrame:
    return (
        pl.DataFrame(
            ohlc_data,
            schema=["timestamp", "open", "high", "low", "close"],
            orient="row"
        )
        .with_columns([
            pl.from_epoch("timestamp", time_unit="ms"),
            pl.lit(asset).alias("asset"),
        ])
    )


def technical_indicators(
    df: pl.DataFrame
) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("close").ta.ema(5).alias("ema5"),
    ])
    return df
    

def transform(raw_data: List[Dict]) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []

    for coin_data in raw_data:
        coin = coin_data["coin"]
        ohlc = coin_data["data"]

        ohlc_df = _ohlc_to_df(ohlc, coin)

        df = technical_indicators(ohlc_df)


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
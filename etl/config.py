import os
import logging

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

class Config:
    COIN_LIST = str(os.getenv("COIN_LIST")).split(",")
    DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))
    DATA_DIR = os.getenv("DATA_DIR", "data/")

    @classmethod
    def display_config(cls):
        print(f"Coins to process: {cls.COIN_LIST}")
        print(f"Days back: {cls.DAYS_BACK}")
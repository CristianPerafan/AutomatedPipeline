import os
import logging

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

class Config:
    ASSETS_LIST = str(os.getenv("ASSET_LIST")).split(",")
    DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))

    @classmethod
    def display_config(cls):
        print(f"Assets to process: {cls.ASSETS_LIST}")
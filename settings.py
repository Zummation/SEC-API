import os
from dotenv import load_dotenv

load_dotenv()

FUND_FACTSHEET_KEY = os.getenv("FUND_FACTSHEET_KEY")
FUND_DAILY_INFO_KEY = os.getenv("FUND_DAILY_INFO_KEY")
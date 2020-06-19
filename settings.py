import os
from dotenv import load_dotenv

load_dotenv()

FUND_FACTSHEET_KEY = eval(os.getenv("FUND_FACTSHEET_KEY"))
FUND_DAILY_INFO_KEY = eval(os.getenv("FUND_DAILY_INFO_KEY"))
from investment_buddy.scraper import scrape_metrics
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

df_filtered = pd.read_excel("data/df_latest_filtered.xlsx")
scrape_metrics(df_filtered, "latest")

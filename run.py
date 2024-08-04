from investment_buddy.downloader import NseDownloader, BseDownloader
from investment_buddy.filterer import DataFilters
from investment_buddy.scraper import scrape_metrics
import logging
import pendulum

logging.basicConfig(level=logging.INFO)

nse_downloader = NseDownloader()
bse_downloader = BseDownloader()

nse_downloader.update_data(prune_weeks=80)
bse_downloader.update_data(prune_weeks=80)

as_of_date = pendulum.today()  # pendulum.from_format(f"20220228", "YYYYMMDD")
data_filter = DataFilters(as_of_date)
data_filter.apply_all_filters()

scrape_metrics(data_filter.df_all_filtered, data_filter.date_str)

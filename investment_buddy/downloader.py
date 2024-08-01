import requests
import pendulum
from pendulum import today, Date
from pathlib import Path
from typing import Union
import zipfile
import tempfile
import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import logging
from requests.exceptions import HTTPError, ReadTimeout, Timeout
import glob
import numpy as np

logger = logging.getLogger(__name__)


class StockDownloader(object):
    def __init__(self, timeout: int = 2):
        self.timeout = timeout
        if len(glob.glob(f"{self.download_path}/*.csv")) == 0:
            self.download_past_two_years()

    def download_data_for_date(self, date: Date, replace=False):
        download_url = self.make_url_func(date)
        file_name = date.format("YYYYMMDD") + ".csv"
        fd = None
        if file_name not in os.listdir(self.download_path) or replace:
            try:
                r = requests.get(
                    download_url,
                    allow_redirects=True,
                    timeout=self.timeout,
                    headers={"User-Agent": "firefox"},
                )
                r.raise_for_status()

                if self.exchange == "NSE":
                    fd, name = tempfile.mkstemp(suffix=".zip")
                    with open(name, "wb") as f:
                        f.write(r.content)
                    zipdata = zipfile.ZipFile(name)
                    zipinfos = zipdata.infolist()
                    for zipinfo in zipinfos:
                        zipinfo.filename = file_name
                        zipdata.extract(zipinfo, self.download_path)
                else:
                    with open(self.download_path / file_name, "wb") as f:
                        f.write(r.content)

                remap = {
                    "TckrSymb": "symbol",
                    "ISIN": "isin",
                    "Src": "exchange",
                    "OpnPric": "open",
                    "HghPric": "high",
                    "LwPric": "low",
                    "ClsPric": "close",
                    "TtlTradgVol": "volume",
                    "FinInstrmId": "alt_id",
                }
                col_order = [
                    "symbol",
                    "isin",
                    "alt_id",
                    "exchange",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "year",
                    "month",
                    "day",
                    "ym",
                ]

                cond = (
                    'sctysrs=="EQ"'
                    if self.exchange == "NSE"
                    else '~sctysrs.isin(["E", "F", "G", "MT"])'
                )

                df = (
                    pd.read_csv(self.download_path / file_name)
                    # .pipe(self.reformat)
                    .rename(columns=remap)
                    .assign(
                        date=date.date(),
                        year=date.year,
                        month=date.month,
                        day=date.day,
                        ym=f"{date.year}{date.month:02}",
                    )
                    .rename(columns=str.lower)
                    .query(cond)
                    .loc[:, col_order]
                )
                df.to_csv(self.download_path / file_name, index=False)
                logger.info(
                    f"Downloaded {self.exchange} data for {date.format('DD MMM, YYYY.')}"
                )

            except HTTPError as err:
                if err.response.status_code == 404:
                    logger.info(
                        f"No {self.exchange} data available on {date.format('DD MMM, YYYY.')}"
                    )
            except (ReadTimeout, Timeout) as err:
                logger.info(
                    f"No {self.exchange} data available on {date.format('DD MMM, YYYY.')}"
                )
            except Exception as err:
                logger.warning(
                    f"{self.exchange} data not available on {date.format('DD MMM, YYYY.')}"
                )
                logger.warning(err)
            finally:
                if self.exchange == "NSE":
                    if fd:
                        os.close(fd)
        else:
            logger.info(
                f"{self.exchange} data for {date.format('DD MMM, YYYY.')} already present"
            )

    def prune_data(self, prune_weeks):
        thresh = int(today().subtract(weeks=prune_weeks).format("YYYYMMDD"))
        files_to_prune = [
            self.download_path / f"{d}.csv" for d in self.days_present if d < thresh
        ]
        for file in files_to_prune:
            os.remove(file)

    @property
    def days_present(self):
        return [
            int(d.replace(".csv", "")) for d in glob.glob1(self.download_path, "*.csv")
        ]

    def update_data(self, prune_weeks=0):
        start_date = pendulum.from_format(str(max(self.days_present)), "YYYYMMDD").add(
            days=1
        )
        if start_date < today():
            self.download_date_range(start_date, today())
        if prune_weeks:
            self.prune_data(prune_weeks)

    def download_date_range(self, start_date: Date, end_date: Date):
        assert start_date < end_date, "Start must be before end"
        dates = pd.date_range(start_date.date(), end_date.date(), freq="B").tolist()
        dates = [
            pendulum.DateTime(d.date().year, d.date().month, d.date().day)
            for d in dates
            if not d.strftime("%Y%m%d") in self.exclude_days
        ]
        for dt in dates:
            self.download_data_for_date(dt)

        # Multiprocessing seems to be causing issues and might be overkill anyway considering bot is going
        # to be running on a daily basis.
        # with ProcessPoolExecutor() as executor:
        #     executor.map(self.download_data_for_date, dates)

    def download_past_two_years(self):
        self.download_date_range(today().subtract(years=2), today())

    def download_last_n_weeks(self, n_weeks):
        self.download_date_range(today().subtract(weeks=n_weeks), today())


class NseDownloader(StockDownloader):
    download_path = Path("data/nse")
    exchange = "NSE"
    exclude_days = []

    def make_url_func(self, date: Date):
        date_str = date.format("YYYYMMDD").upper()
        return f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"

    def reformat(self, df):
        return df.rename(columns={"TOTTRDQTY": "volume"}).assign(exchange=self.exchange)


class BseDownloader(StockDownloader):
    download_path = Path("data/bse")
    exchange = "BSE"
    exclude_days = ["20211229"]

    def make_url_func(self, date: Date):
        date_str = date.format("YYYYMMDD").upper()
        return f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date_str}_F_0000.CSV"

    def reformat(self, df):
        return df.rename(
            columns={
                "NO_OF_SHRS": "volume",
                "SC_NAME": "symbol",
                "SC_CODE": "isin",
                "SC_TYPE": "series",
            }
        ).assign(
            exchange=self.exchange,
            series=lambda df: np.where(df.series == "Q", "EQ", df.series),
        )

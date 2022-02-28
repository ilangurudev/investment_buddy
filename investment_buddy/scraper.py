import requests
from typing import Union
import os
import pandas as pd
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows

logger = logging.getLogger(__name__)
tqdm.pandas()
os.environ["WDM_LOG_LEVEL"] = "0"


class PageFinder(object):
    """ """

    base_search_url = "http://www.moneycontrol.com/stocks/cptmarket/compsearchnew.php?topsearch_type=1&search_str="

    keys_dict = pd.read_csv("data/keys.csv").set_index("field").to_dict(orient="index")
    keys_dict = {k: v["identifier"] for k, v in keys_dict.items()}
    check_element = keys_dict["market_cap"].replace(".", "")

    options = webdriver.ChromeOptions()
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--incognito")
    options.add_argument("--headless")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    browser = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    def __init__(self, isin, symbol):
        self.isin, self.symbol = str(isin), str(symbol)
        self.url = self.home_content = self.ratios_url = None
        self.props = dict()
        self.try_finding_info()

    def make_url(self, term):
        return self.base_search_url + term

    def get_parsed_content(self, url, wait_time=0):
        self.browser.implicitly_wait(wait_time)
        self.browser.get(url)
        return BeautifulSoup(self.browser.page_source, features="lxml")

    def validate_and_gather_info(self, term):
        url = self.make_url(term)
        content = requests.get(url).content
        if self.check_element in str(content):
            logger.info(f"\nGathering Data for {self.symbol}")
            self.url = url
            soup_home = self.get_parsed_content(url, 10)
            market_cap = soup_home.select_one(
                self.keys_dict["market_cap"].replace(" ", ".")
            ).text.replace(",", "")
            self.props["market_cap"] = float(market_cap)
            #             self.props["BLANK"] = ""

            self.get_ratios(soup_home)
            self.get_financials()
            return True
        else:
            return False

    def parse_series(self, content, selector):
        ls = []
        for x in content.select(self.keys_dict[selector]):
            try:
                f = float(x.text.replace(",", ""))
                ls.append(f)
            except:
                ls.append(None)
        if len(ls) < 5:
            ls += [None] * (5 - len(ls))
        return ls

    def get_ratios(self, soup):
        self.standalone_ratios_url = soup.select_one(self.keys_dict["ratios_url"])[
            "href"
        ]
        self.consolidated_ratios_url = self.standalone_ratios_url.replace(
            "ratiosVI", "consolidated-ratiosVI"
        )
        for name, url in zip(
            ["consolidated", "standalone"],
            [self.consolidated_ratios_url, self.standalone_ratios_url],
        ):
            content = self.get_parsed_content(url)
            for metric in ["rnw", "de"]:
                self.props[f"{name}_{metric}"] = self.parse_series(
                    content, f"{name}_{metric}"
                )

    def get_financials(self):
        self.standlone_financials_url = self.standalone_ratios_url.replace(
            "ratiosVI", "results/yearly"
        )
        self.consolidated_financials_url = self.consolidated_ratios_url.replace(
            "consolidated-ratiosVI", "results/consolidated-yearly"
        )

        for name, url in zip(
            ["consolidated", "standalone"],
            [self.consolidated_financials_url, self.standlone_financials_url],
        ):
            content = self.get_parsed_content(url)
            for metric in ["sr", "np"]:
                self.props[f"{name}_{metric}"] = self.parse_series(
                    content, f"{name}_{metric}"
                )

    def try_finding_info(self):
        search_terms = [self.isin] + [
            self.symbol[:i] for i in range(len(self.symbol), 3, -1)
        ]
        for term in search_terms:
            if self.validate_and_gather_info(term):
                logger.info(f"Found data for {self.symbol}")
                return
        logger.warning(f"\nCould not find data for {self.symbol}")
        # self.browser.close()

    def __repr__(self):
        return f"PageFinder({self.isin}, {self.symbol}, {self.url})"


def scrape_metrics(df_filtered, date_str):
    df_filtered = df_filtered.assign(
        pf=lambda df: df.progress_apply(
            lambda row: PageFinder(row["isin"], row["symbol"]), axis=1
        ),
    )
    # we query only for stocks where data was successfully scraped as they are easier to split into columns
    # This works even during merging with the df_filtered because the indix is left unchanged and that makes
    # sure the alignment happens correctly. 
    df = (
        df_filtered.pf.apply(lambda pf: pf.props)
        .apply(pd.Series)
        .query("market_cap.notna()", engine="python")
    )
    cols = [
        "consolidated_rnw",
        "standalone_rnw",
        "consolidated_sr",
        "consolidated_np",
        "standalone_sr",
        "standalone_np",
        "consolidated_de",
        "standalone_de",
    ]
    for b, col in enumerate(cols):
        names = [f"{col.replace('_', ' ')}{i}" for i in range(5, 0, -1)]
        df[names] = pd.DataFrame(df[col].tolist(), index=df.index)
        df[f"BLANK {b}"] = ""
    df = df.drop(columns=cols)

    # index of df and df_filtered makes sure alignement happens correctly
    df_final = (
        pd.concat(
            [
                df_filtered[["symbol", "isin", "exchange", "filter"]],
                df,
                df_filtered[["date_str"]],
            ],
            axis=1,
        )
        .assign(no_data=lambda df: df.market_cap.isna())
        .sort_values("no_data")
    )

    df_final.loc[~df_final.no_data] = df_final.loc[~df_final.no_data].fillna("")
    df_final.loc[df_final.no_data] = df_final.loc[df_final.no_data].fillna(
        "No Match in Money Control"
    )
    df_final = df_final.drop(columns="no_data").rename(columns=str.upper)

    # Load the file
    wb = openpyxl.load_workbook("data/results_template.xlsx")
    ws = wb.active

    # Convert the dataframe into rows
    rows = dataframe_to_rows(df_final, index=False, header=False)

    # Write the rows to the worksheet
    for r_idx, row in enumerate(rows, 2):
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    # Save the worksheet as a (*.xlsx) file
    wb.template = False
    save_path = f"data/{date_str}.xlsx" if date_str=="latest" else f"data/final/{date_str}.xlsx"
    wb.save(save_path)
    logger.info(f"Saved data to {save_path}")

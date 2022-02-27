import scrapy
import pandas as pd

from scrapy_splash import SplashRequest
import scrapy_splash
# from scrapy_selenium import SeleniumRequest




class MoneyControlSpider(scrapy.Spider):
    name = "quotes"
    keys_dict = pd.read_csv("../data/keys.csv").set_index("field").to_dict(orient="index")
    keys_dict = {k: v["identifier"] for k, v in keys_dict.items()}

    def start_requests(self):
        df = pd.read_excel("../data/df_filtered_valid.xlsx")
        for isin, url in zip(df["isin"].tolist(), df.url.tolist()):
            # yield SplashRequest(url=url, callback=self.parse, args={"wait":2}, cb_kwargs=dict(identifier=f"{isin}"))
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(identifier=f"{isin}"))

            
    def parse(self, response, identifier):
        market_cap = response.css(self.keys_dict["market_cap"].replace(" ", ".") + "::text").get()
        with open(f"../data/scraped/{identifier}.txt", 'w') as f:
            f.write(market_cap)
        self.log(f'{identifier} Market Cap: {market_cap}')
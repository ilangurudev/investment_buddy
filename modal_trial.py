import modal
import pickle
import pandas as pd
import requests
from bs4 import BeautifulSoup

app = modal.App("scraping-investnments")

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("requests")
    .pip_install("beautifulsoup4")
    .pip_install("pandas")
    .pip_install("lxml")
)


def scrape_mc_index():
    letters = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
    letters.append("Others")
    letters = [""] + letters

    all_links = []

    for letter in letters:
        print(letter)
        resp = requests.get(
            f"https://www.moneycontrol.com/india/stockpricequote/{letter}"
        )
        home = BeautifulSoup(resp.content, features="lxml")
        td_elements = home.find_all(class_="bl_12")

        for element in td_elements:
            text = element.text
            href = element.attrs["href"]
            all_links.append((text, href))

    all_links_new = []
    names = []
    for name, url in all_links:
        if name not in names:
            all_links_new.append((name, url))
            names.append(name)

    len(all_links), len(all_links_new)

    with open("data/all_companies.pkl", "wb") as f:
        pickle.dump(all_links_new, f)


@app.function(image=image)
def scrape_company_details(page_name, page_url):
    import requests
    from bs4 import BeautifulSoup

    company_info = {"name": page_name, "url": page_url}

    try:
        # if True:
        page_content = requests.get(page_url).content
        page_soup = BeautifulSoup(page_content, features="lxml")

        comdetl_elements = (
            page_soup.find(id="company_info")
            .find_all(class_="comdetl")[-1]
            .find_all("li")
        )
        for element in comdetl_elements:
            key = element.find("span").text.strip(":")
            value = element.find("p").text
            company_info[key] = value
        return company_info
    except:
        return company_info


@app.local_entrypoint()
def main():
    # scrape_mc_index()
    with open("data/all_companies.pkl", "rb") as f:
        all_links = pickle.load(f)
    total = len(all_links)
    df_old_scraped = pd.read_csv("./data/company_info.csv").rename(columns=str.lower)
    all_links = [
        (name, url)
        for name, url in all_links
        if name not in df_old_scraped.name.tolist()
    ]
    company_info_ls = [deets for deets in scrape_company_details.starmap(all_links)]
    with open("data/all_companies_list.pkl", "wb") as f:
        pickle.dump(company_info_ls, f)
    df_newly_scraped = (
        pd.DataFrame(company_info_ls).query("ISIN.notna()").rename(columns=str.lower)
    )
    print(f"{len(df_newly_scraped)} links newly scraped")
    df_company_info = (
        pd.concat(
            [
                df_old_scraped,
                df_newly_scraped,
            ]
        )
        .sort_values("name")
        .drop_duplicates("name")
    )
    print(f"{len(df_company_info)} out of {total} links scraped in total")
    df_company_info.to_csv("./data/company_info.csv", index=False)

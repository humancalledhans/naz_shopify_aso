import time
from ..items import CollectionRankingRelevance

import random
import scrapy
import re
import hashlib
import pandas as pd

from scrapy import Request

from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver import ActionChains

from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest


def scrape_collection_rankings(collection_page_url, id, end_link_to_append, sort_by_to_append):

    s = Service(ChromeDriverManager().install())
    options = Options()
    options.headless = True
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=s, options=options)

    driver.get(collection_page_url)
    time.sleep(0.5)
    while driver.execute_script("return document.readyState;") != "complete":
        time.sleep(0.5)

    collection_ranking_list = []

    number_regex = re.compile(r"\d+")

    number_of_apps_raw = None

    try:
        number_of_apps_raw = driver.find_element(By.XPATH, "//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third']")
        number_of_apps_raw = number_of_apps_raw.text

    except NoSuchElementException as e:
        scrape_collection_rankings(collection_page_url, id, end_link_to_append, sort_by_to_append)
        
    number_of_apps_processed = re.findall(number_regex, number_of_apps_raw)

    number_of_apps_processed_final = []
    for str_object in number_of_apps_processed:
        number_of_apps_processed_final.append(int(str_object))

    number_of_apps = max(number_of_apps_processed_final)

    app_id_ranking_list = []

    for page_num in range(1,int(number_of_apps)//24+2):

        substr_regex = re.compile(r'https://apps.shopify.com/((\w+)(-\w+)*)')

        link_to_scrape = collection_page_url + end_link_to_append + "&page=" + str(page_num) + sort_by_to_append

        driver.get(link_to_scrape)
        time.sleep(0.5)
        while driver.execute_script("return document.readyState;") != "complete":
            time.sleep(0.5)

        apps_href_list_length = len(driver.find_elements(By.XPATH, "//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third grid-item--app-card-listing']//div[@class='ui-app-card']"))

        for idx in range(apps_href_list_length):
            elem = driver.find_element(By.XPATH, f"(//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third grid-item--app-card-listing']//div[@class='ui-app-card'])[{idx+1}]").get_attribute('data-target-href')
            app_link_first = substr_regex.search(elem)
            app_link = app_link_first.group()
            app_id = hashlib.md5(app_link.lower().encode()).hexdigest()
            app_ranking = idx+1

            app_id_ranking_list.append((app_ranking+((page_num-1)*24), app_id))

    if len(app_id_ranking_list) > 0:
            if "&sort_by=relevance" in driver.current_url:
                return CollectionRankingRelevance(collection_id=id, app_id_list=app_id_ranking_list)


class ColrankrelevanceDataScrapeSpider(scrapy.spiders.SitemapSpider):

    COLLECTIONS_REGEX = r"https://apps.shopify.com/collections/((\w+)(-\w+)*)[^?]"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'colrankrelevance_data_scrape'
    allowed_domains = ['apps.shopify.com']
    sitemap_urls = ['https://apps.shopify.com/sitemap.xml']
    sitemap_rules = [
        (re.compile(COLLECTIONS_REGEX), 'parse_collections')
    ]

    custom_settings = {
        'COOKIES_ENABLED': False,
        'DOWNLOAD_DELAY': 3,
    }

    @staticmethod
    def close(spider, reason):
        spider.logger.info('Spider closed: %s', spider.name)
        spider.logger.info('Preparing unique categories...')

        # Normalise collections_ranking_relevance
        collections_df = pd.read_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/collections_ranking_relevance.csv')
        collections_df.drop_duplicates(subset=['collection_id', 'app_ranking_list']).to_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/collections_ranking_relevance.csv', index=False)

        spider.logger.info('Unique collection rankings are there 👌')

        return super().close(spider, reason)


    def parse_collections(self, response):

        collection_name = response.xpath("//h1[@class='heading--2 ui-app-store-hero__header']//text()").get().strip()
        collection_id = hashlib.md5(collection_name.lower().encode()).hexdigest()

        end_link_to_append = "?app_integration_pos=off&app_integration_shopify_checkout=off"
        sort_by_to_append = ["&pricing=all&requirements=off&sort_by=relevance"]

        for sort_by in sort_by_to_append:
            yield scrape_collection_rankings(collection_page_url=response.url, id=collection_id, end_link_to_append=end_link_to_append, sort_by_to_append=sort_by)

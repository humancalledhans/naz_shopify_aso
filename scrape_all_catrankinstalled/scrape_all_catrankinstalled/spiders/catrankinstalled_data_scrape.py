import time
from ..items import CategoryRankingInstalled

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


def scrape_category_rankings(browse_page_url, id, is_category, end_link_to_append, sort_by_to_append):

    s = Service(ChromeDriverManager().install())
    options = Options()
    options.headless = True
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=s, options=options)

    driver.get(browse_page_url)
    time.sleep(0.5)
    while driver.execute_script("return document.readyState;") != "complete":
        time.sleep(0.5)

    number_regex = re.compile(r"\d+")
    number_of_apps_raw = driver.find_element(By.XPATH, "//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third']")
    number_of_apps_raw = number_of_apps_raw.text
    number_of_apps_processed = re.findall(number_regex, number_of_apps_raw)

    number_of_apps_processed_final = []
    for str_object in number_of_apps_processed:
        number_of_apps_processed_final.append(int(str_object))

    number_of_apps = max(number_of_apps_processed_final)

    end_link_to_append = "?app_integration_pos=off&app_integration_shopify_checkout=off"

    app_id_ranking_list = []

    for page_num in range(1,int(number_of_apps)//24+2):

        substr_regex = re.compile(r'https://apps.shopify.com/((\w+)(-\w+)*)')

        link_to_scrape = browse_page_url + end_link_to_append + "&page=" + str(page_num) + sort_by_to_append

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
        if is_category:
            if "&sort_by=installed" in driver.current_url:
                return CategoryRankingInstalled(category_id=id, app_id_list=app_id_ranking_list)



class CatrankinstalledDataScrapeSpider(scrapy.spiders.SitemapSpider):

    CATEGORIES_REGEX = r"https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'catrankinstalled_data_scrape'
    allowed_domains = ['apps.shopify.com']
    sitemap_urls = ['https://apps.shopify.com/sitemap.xml']
    sitemap_rules = [
        (re.compile(CATEGORIES_REGEX), 'parse_subcategories_of_categories'),
    ]

    custom_settings = {
        'COOKIES_ENABLED': False,
        'DOWNLOAD_DELAY': 3,
    }

    @staticmethod
    def close(spider, reason):
        spider.logger.info('Spider closed: %s', spider.name)
        spider.logger.info('Preparing unique categories...')

        # Normalize category_ranking_most_installed
        categories_df = pd.read_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/category_ranking_installed.csv')
        categories_df.drop_duplicates(subset=['category_id', 'app_id_list']).to_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/category_ranking_installed.csv', index=False)

        spider.logger.info('Unique category rankings are there 👌')

        return super().close(spider, reason)


    def parse_subcategories_of_categories(self, response):
        
        url_obtained = response.url

        all_the_category_urls = ['https://apps.shopify.com/categories/store-design', 'https://apps.shopify.com/categories/marketing', 'https://apps.shopify.com/categories/conversion',
            'https://apps.shopify.com/categories/customer-service', 'https://apps.shopify.com/categories/sourcing-and-selling-products', 'https://apps.shopify.com/categories/store-management',
            'https://apps.shopify.com/categories/merchandising', 'https://apps.shopify.com/categories/fulfillment', 'https://apps.shopify.com/categories/shipping-and-delivery']

        if url_obtained in all_the_category_urls:
            substr_regex = re.compile(r'https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]')
            browse_link_first = substr_regex.search(url_obtained)

            url_obtained_browse_page = browse_link_first.group().replace('https://apps.shopify.com/categories/', 'https://apps.shopify.com/browse/')

            yield Request(url=url_obtained_browse_page, callback=self.parse_category_browse_page_for_description)


    def parse_category_browse_page_for_description(self, response):

        try:
            category_description = response.xpath("//p[@class='text-major ui-app-store-hero__description']//text()").get().strip()
        except AttributeError:
            category_description = ''

        category_name = response.xpath("//h1//text()").get().strip()
        category_id = hashlib.md5(category_name.lower().encode()).hexdigest()

        end_link_to_append = "?app_integration_pos=off&app_integration_shopify_checkout=off"
        sort_by_to_append = ["&pricing=all&requirements=off&sort_by=installed"]

        for sort_by in sort_by_to_append:
            yield scrape_category_rankings(browse_page_url=response.url, id=category_id, is_category=True, end_link_to_append = end_link_to_append, sort_by_to_append=sort_by)


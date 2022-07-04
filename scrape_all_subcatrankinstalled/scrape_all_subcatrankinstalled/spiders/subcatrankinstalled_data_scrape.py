import time
from ..items import SubCategoryRankingInstalled

import scrapy
import re
import hashlib

from scrapy import Request

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


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
    number_of_apps_raw = driver.find_element(
        By.XPATH, "//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third']")
    number_of_apps_raw = number_of_apps_raw.text
    number_of_apps_processed = re.findall(number_regex, number_of_apps_raw)

    number_of_apps_processed_final = []
    for str_object in number_of_apps_processed:
        number_of_apps_processed_final.append(int(str_object))

    number_of_apps = max(number_of_apps_processed_final)

    end_link_to_append = "?app_integration_pos=off&app_integration_shopify_checkout=off"

    app_id_ranking_list = []

    for page_num in range(1, int(number_of_apps)//24+2):

        substr_regex = re.compile(r'https://apps.shopify.com/((\w+)(-\w+)*)')

        link_to_scrape = browse_page_url + end_link_to_append + \
            "&page=" + str(page_num) + sort_by_to_append

        driver.get(link_to_scrape)
        time.sleep(0.5)
        while driver.execute_script("return document.readyState;") != "complete":
            time.sleep(0.5)

        apps_href_list_length = len(driver.find_elements(
            By.XPATH, "//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third grid-item--app-card-listing']//div[@class='ui-app-card']"))

        for idx in range(apps_href_list_length):
            elem = driver.find_element(
                By.XPATH, f"(//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third grid-item--app-card-listing']//div[@class='ui-app-card'])[{idx+1}]").get_attribute('data-target-href')
            app_link_first = substr_regex.search(elem)
            app_link = app_link_first.group()
            app_id = hashlib.md5(app_link.lower().encode()).hexdigest()
            app_ranking = idx+1

            app_id_ranking_list.append((app_ranking+((page_num-1)*24), app_id))

    if len(app_id_ranking_list) > 0:
        if not is_category:
            if "&sort_by=installed" in driver.current_url:
                for app_id in app_id_ranking_list:
                    yield SubCategoryRankingInstalled(subcategory_id=id, rank=app_id[0], app_id=app_id[1])


class SubcatrankinstalledDataScrapeSpider(scrapy.spiders.SitemapSpider):

    CATEGORIES_REGEX = r"https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]"
    COLLECTIONS_REGEX = r"https://apps.shopify.com/collections/((\w+)(-\w+)*)[^?]"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'scrape_all_subcatrankinstalled'

    allowed_domains = ['apps.shopify.com']
    sitemap_urls = ['https://apps.shopify.com/sitemap.xml']
    sitemap_rules = [
        (re.compile(CATEGORIES_REGEX), 'parse_subcategories_of_categories'),
    ]

    custom_settings = {
        'COOKIES_ENABLED': False,
        # this should be the default parameter, when actually running the code.
        'DOWNLOAD_DELAY': 3,
    }

    def parse_subcategories_of_categories(self, response):

        url_obtained = response.url

        all_the_category_urls = ['https://apps.shopify.com/categories/store-design', 'https://apps.shopify.com/categories/marketing', 'https://apps.shopify.com/categories/conversion',
                                 'https://apps.shopify.com/categories/customer-service', 'https://apps.shopify.com/categories/sourcing-and-selling-products', 'https://apps.shopify.com/categories/store-management',
                                 'https://apps.shopify.com/categories/merchandising', 'https://apps.shopify.com/categories/fulfillment', 'https://apps.shopify.com/categories/shipping-and-delivery']

        if url_obtained not in all_the_category_urls:
            substr_regex = re.compile(
                r'https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]')
            browse_link_first = substr_regex.search(url_obtained)

            browse_page_url = browse_link_first.group().replace(
                'https://apps.shopify.com/categories/', 'https://apps.shopify.com/browse/')

            return Request(url=browse_page_url, callback=self.parse_subcategory_browse_page_for_apps)

    def parse_subcategory_browse_page_for_apps(self, response):
        apps_href_list = response.xpath(
            "//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third grid-item--app-card-listing']//div[@class='ui-app-card']//@href").getall()

        category_name = response.xpath("//h1//text()").get().strip()
        category_id = hashlib.md5(category_name.lower().encode()).hexdigest()

        end_link_to_append = "?app_integration_pos=off&app_integration_shopify_checkout=off"
        sort_by_to_append = ["&pricing=all&requirements=off&sort_by=installed"]

        for sort_by in sort_by_to_append:
            return scrape_category_rankings(browse_page_url=response.url, id=category_id, is_category=False, end_link_to_append=end_link_to_append, sort_by_to_append=sort_by)

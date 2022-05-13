### to run: scrapy crawl category_data_scrape

from ..items import SubCategory

import random
import scrapy
import re
import hashlib
import pandas as pd

from scrapy import Request

from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest

class SubcategoryDataScrapeSpider(scrapy.spiders.SitemapSpider):
    CATEGORIES_REGEX = r"https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'subcategory_data_scrape'

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
        spider.logger.info('Preparing unique subcategories...')

        subcategories_df = pd.read_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/subcategories.csv')
        subcategories_df.drop_duplicates(subset=['parent_category_name', 'parent_category_id', 'subcategory_name', 'subcategory_id']).to_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/subcategories.csv', index=False)

        spider.logger.info('Unique subcategories are there 👌')

        return super().close(spider, reason)

    def parse_subcategories_of_categories(self, response):
        
        url_obtained = response.url 

        all_the_category_urls = ['https://apps.shopify.com/categories/store-design', 'https://apps.shopify.com/categories/marketing', 'https://apps.shopify.com/categories/conversion',
            'https://apps.shopify.com/categories/customer-service', 'https://apps.shopify.com/categories/sourcing-and-selling-products', 'https://apps.shopify.com/categories/store-management',
            'https://apps.shopify.com/categories/merchandising', 'https://apps.shopify.com/categories/fulfillment', 'https://apps.shopify.com/categories/shipping-and-delivery']

        if url_obtained not in all_the_category_urls:
            substr_regex = re.compile(r'https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]')
            browse_link_first = substr_regex.search(url_obtained)

            browse_page_url = browse_link_first.group().replace('https://apps.shopify.com/categories/', 'https://apps.shopify.com/browse/')

            yield Request(url=browse_page_url, callback=self.parse_subcategory_browse_page_for_apps)


    def parse_subcategory_browse_page_for_apps(self, response):
        
        category_name = response.xpath("//h1//text()").get().strip()
        category_id = hashlib.md5(category_name.lower().encode()).hexdigest()

        subcategory_name = response.xpath("((//button[@class='marketing-button applied-filters__filter'])[2]//text())[1]").get().strip()
        subcategory_id = hashlib.md5(subcategory_name.lower().encode()).hexdigest()

        try:
            subcategory_description = response.xpath("//p[@class='text-major ui-app-store-hero__description']//text()").get()
        except AttributeError:
            subcategory_description = ''

        yield SubCategory(
            parent_category_name = category_name.lower(),
            parent_category_id = category_id,
            subcategory_name = subcategory_name.strip(),
            subcategory_id = subcategory_id,
            subcategory_description = subcategory_description
            )

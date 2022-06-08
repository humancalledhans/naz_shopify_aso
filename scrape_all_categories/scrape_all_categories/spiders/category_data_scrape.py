### to run: scrapy crawl category_data_scrape

from ..items import Category

from datetime import date

import random
import scrapy
import re
import hashlib
import pandas as pd

from scrapy import Request

from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest

class CategoryDataScrapeSpider(scrapy.spiders.SitemapSpider):
    CATEGORIES_REGEX = r"https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'scrape_all_categories'

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

        categories_df = pd.read_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/categories.csv')
        categories_df.drop_duplicates(subset=['id', 'category_title']).to_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/categories.csv', index=False)

        spider.logger.info('Unique categories are there 👌')

        return super().close(spider, reason)

    def parse_subcategories_of_categories(self, response):
        
        url_obtained = response.url

        all_the_category_urls = ['https://apps.shopify.com/categories/store-design', 'https://apps.shopify.com/categories/marketing', 'https://apps.shopify.com/categories/conversion',
            'https://apps.shopify.com/categories/customer-service', 'https://apps.shopify.com/categories/sourcing-and-selling-products', 'https://apps.shopify.com/categories/store-management',
            'https://apps.shopify.com/categories/merchandising', 'https://apps.shopify.com/categories/fulfillment', 'https://apps.shopify.com/categories/shipping-and-delivery']

        if url_obtained in all_the_category_urls:

            substr_regex = re.compile(r'https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]')
            browse_link_first = substr_regex.search(url_obtained)

            url_obtained_browse_page = browse_link_first.group().replace('https://apps.shopify.com/categories/', 'https://apps.shopify.com/browse/') # output would be the browse page url of the subcategory. (yep, it would be.)

            yield Request(url=url_obtained_browse_page, callback=self.parse_category_browse_page_for_description)


    def parse_category_browse_page_for_description(self, response):

        try:
            category_description = response.xpath("//p[@class='text-major ui-app-store-hero__description']//text()").get().strip()
        except AttributeError:
            category_description = ''

        category_name = response.xpath("//h1//text()").get().strip()
        category_id = hashlib.md5(category_name.lower().encode()).hexdigest()

        scraped_date = date.today()

        yield Category(category_id=category_id, category_title=category_name, category_description=category_description)

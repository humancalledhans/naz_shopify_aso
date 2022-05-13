from ..items import Collection

import scrapy
import re
import hashlib
import pandas as pd

from scrapy import Request

from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest


class CollectionsDataScrapeSpider(scrapy.spiders.SitemapSpider):
    COLLECTIONS_REGEX = r"https://apps.shopify.com/collections/((\w+)(-\w+)*)[^?]"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'collections_data_scrape'
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
        spider.logger.info('Preparing unique collections...')

        # Normalise collections
        collections_df = pd.read_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/collections.csv')
        collections_df.drop_duplicates(subset=['collection_id', 'collection_title', 'collection_description']).to_csv('/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/collections.csv', index=False)

        spider.logger.info('Unique collections are there 👌')

        return super().close(spider, reason)


    def parse_collections(self, response):

        collection_name = response.xpath("//h1[@class='heading--2 ui-app-store-hero__header']//text()").get().strip()
        collection_id = hashlib.md5(collection_name.lower().encode()).hexdigest()

        try:
            collection_description = response.xpath("//p[@class='text-major ui-app-store-hero__description']//text()").get().strip()
        except AttributeError:
            collection_description = ""

        yield Collection(
            collection_id = collection_id,
            collection_title =  collection_name,
            collection_description = collection_description
            )

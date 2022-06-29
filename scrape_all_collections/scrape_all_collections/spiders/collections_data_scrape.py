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

    name = 'scrape_all_collections'
    allowed_domains = ['apps.shopify.com']
    sitemap_urls = ['https://apps.shopify.com/sitemap.xml']

    sitemap_rules = [
        (re.compile(COLLECTIONS_REGEX), 'parse_collections')
    ]

    custom_settings = {
        'COOKIES_ENABLED': False,
        'DOWNLOAD_DELAY': 3,
    }

    def parse_collections(self, response):

        collection_name = response.xpath(
            "//h1[@class='heading--2 ui-app-store-hero__header']//text()").get().strip()
        collection_id = hashlib.md5(
            collection_name.lower().encode()).hexdigest()

        try:
            collection_description = response.xpath(
                "//p[@class='text-major ui-app-store-hero__description']//text()").get().strip()
        except AttributeError:
            collection_description = ""

        yield Collection(
            collection_id=collection_id,
            collection_title=collection_name,
            collection_description=collection_description
        )

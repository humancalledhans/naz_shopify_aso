# scrapy crawl categoryapp_data_scrape --output "feed/%(name)s-%(time)s.json" --output-format json

from ..items import CategoryApp

import scrapy
import re
import hashlib

from scrapy import Request


class CategoryappDataScrapeSpider(scrapy.spiders.SitemapSpider):
    APP_PAGE_REGEX = r"https://apps.shopify.com/((\w+)(-\w+)*)$"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'categoryapp_data_scrape'

    allowed_domains = ['apps.shopify.com']
    sitemap_urls = ['https://apps.shopify.com/sitemap.xml']

    sitemap_rules = [
        (re.compile(APP_PAGE_REGEX), 'parse_app_page'),
    ]

    custom_settings = {
        'COOKIES_ENABLED': False,
        'DOWNLOAD_DELAY': 3,
    }


    def parse_app_page(self, response):

        app_url = response.url
        app_id = hashlib.md5(app_url.lower().encode()).hexdigest()


        categories_list = []
        for category in response.css('.vc-app-listing-hero__taxonomy-links a::text').extract():
            category_id = hashlib.md5(category.lower().encode()).hexdigest()
            categories_list.append(category_id)

            yield CategoryApp(category_id=category_id, app_id=app_id)


# scrapy crawl keybenefit_data_scrape --output "feed/%(name)s-%(time)s.json" --output-format json

from ..items import KeyBenefit

import scrapy
import re
import hashlib

from scrapy import Request


class KeybenefitScrapeSpider(scrapy.spiders.SitemapSpider):
    APP_PAGE_REGEX = r"https://apps.shopify.com/((\w+)(-\w+)*)$"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'keybenefit_data_scrape'

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

        for benefit in response.css('.vc-app-listing-key-values__item'):
            yield KeyBenefit(app_id=app_id,
                             title=benefit.css('.vc-app-listing-key-values__item-title ::text').extract_first().strip(),
                             benefit_description=benefit.css(
                                 '.vc-app-listing-key-values__item-description ::text').extract_first().strip())

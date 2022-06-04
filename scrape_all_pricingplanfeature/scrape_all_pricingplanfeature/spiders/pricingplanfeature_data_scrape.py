# scrapy crawl app_data_scrape --output "feed/%(name)s-%(time)s.json" --output-format json

from ..items import PricingPlanFeature

import scrapy
import re
import hashlib

from scrapy import Request


class PricingplanfeatureDataScrapeSpider(scrapy.spiders.SitemapSpider):
    APP_PAGE_REGEX = r"https://apps.shopify.com/((\w+)(-\w+)*)$"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'pricingplanfeature_data_scrape'

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

        for pricing_plan in response.css('.ui-card.pricing-plan-card'):
            pricing_plan_id = hashlib.md5(
                pricing_plan.upper().encode()).hexdigest()

            for feature in pricing_plan.css('ul li.bullet'):
                yield PricingPlanFeature(app_id=app_id, pricing_plan_id=pricing_plan_id,
                                         feature_description=' '.join(feature.css('::text').extract()).strip())

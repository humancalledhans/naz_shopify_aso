# scrapy crawl app_data_scrape --output "feed/%(name)s-%(time)s.json" --output-format json

from ..items import PricingPlan

import scrapy
import re
import hashlib

from scrapy import Request


class PricingplanDataScrapeSpider(scrapy.spiders.SitemapSpider):
    APP_PAGE_REGEX = r"https://apps.shopify.com/((\w+)(-\w+)*)$"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'scrape_all_pricingplans'

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
                str(pricing_plan).encode()).hexdigest()
            yield PricingPlan(pricing_plan_id=pricing_plan_id,
                              app_id=app_id,
                              pricing_plan_title=pricing_plan.css('.pricing-plan-card__title-kicker ::text').extract_first(
                                  default='').strip(),
                              price=pricing_plan.css('h3 ::text').extract_first().strip())

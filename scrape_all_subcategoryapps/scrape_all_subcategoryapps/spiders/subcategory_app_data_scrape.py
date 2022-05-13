### to run: scrapy crawl category_data_scrape

from ..items import SubCategoryApp

import random
import scrapy
import re
import hashlib
import pandas as pd

from scrapy import Request

from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest

class SubcategoryappDataScrapeSpider(scrapy.spiders.SitemapSpider):
    CATEGORIES_REGEX = r"https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'subcategoryapp_data_scrape'

    allowed_domains = ['apps.shopify.com']
    sitemap_urls = ['https://apps.shopify.com/sitemap.xml']

    sitemap_rules = [
        (re.compile(CATEGORIES_REGEX), 'parse_subcategories_of_categories'),
    ]

    custom_settings = {
        'COOKIES_ENABLED': False,
        'DOWNLOAD_DELAY': 3,
    }

    def parse_subcategories_of_categories(self, response):
        
        url_obtained = response.url

        all_the_category_urls = ['https://apps.shopify.com/categories/store-design', 'https://apps.shopify.com/categories/marketing', 'https://apps.shopify.com/categories/conversion',
            'https://apps.shopify.com/categories/customer-service', 'https://apps.shopify.com/categories/sourcing-and-selling-products', 'https://apps.shopify.com/categories/store-management',
            'https://apps.shopify.com/categories/merchandising', 'https://apps.shopify.com/categories/fulfillment', 'https://apps.shopify.com/categories/shipping-and-delivery']

        if url_obtained not in all_the_category_urls:
            substr_regex = re.compile(r'https://apps.shopify.com/categories/((\w+)(-\w+)*)[^?]')
            browse_link_first = substr_regex.search(url_obtained)

            browse_page_url = browse_link_first.group().replace('https://apps.shopify.com/categories/', 'https://apps.shopify.com/browse/') # output would be the browse page url of the subcategory. (yep, it would be.)

            yield Request(url=browse_page_url, callback=self.parse_subcategory_apps) 


    def parse_subcategory_apps(self, response):
        apps_href_list = response.xpath("//div[@class='grid__item grid__item--tablet-up-half grid__item--wide-up-third grid-item--app-card-listing']//div[@class='ui-app-card']//@href").getall()

        subcategory_name = response.xpath("((//button[@class='marketing-button applied-filters__filter'])[2]//text())[1]").get().strip()
        subcategory_id = hashlib.md5(subcategory_name.lower().encode()).hexdigest()

        substr_regex = re.compile(r'https://apps.shopify.com/((\w+)(-\w+)*)')

        for href in apps_href_list:
            app_link_first = substr_regex.search(href)
            app_link = app_link_first.group()
            app_id = hashlib.md5(app_link.lower().encode()).hexdigest()

            yield SubCategoryApp(
                    subcategory_id = subcategory_id,
                    app_id = app_id
                )

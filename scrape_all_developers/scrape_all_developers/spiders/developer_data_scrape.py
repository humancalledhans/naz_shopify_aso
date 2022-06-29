# scrapy crawl developer_data_scrape --output "feed/%(name)s-%(time)s.json" --output-format json

from ..items import Developer, DevelopedAppsMediator

import scrapy
import re
import uuid
import hashlib

from scrapy import Request


class DeveloperDataScrapeSpider(scrapy.spiders.SitemapSpider):
    APP_PAGE_REGEX = r"https://apps.shopify.com/((\w+)(-\w+)*)$"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'scrape_all_developers'

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

        support_section_list = response.xpath(
            "(//ul[@class='vc-app-listing-support-section__list'])[1]//text()[not(parent::style)]").getall()
        about_developer_section_list = response.xpath(
            "(//ul[@class='vc-app-listing-support-section__list'])[3]//text()[not(parent::style)]").getall()

        support_email_boolean = False

        get_support_boolean = False

        website_exists_boolean = False

        support_email = None
        support_phone = None
        dev_average_rating = None
        dev_partners_href = None
        dev_experience = None
        dev_website = None

        for elem in support_section_list:
            if 'Get support' in elem:
                get_support_boolean = True

            if get_support_boolean:
                support_email_regex = re.compile(r"(.)(.)+@(.)(.)+")
                support_email_processed = support_email_regex.search(elem)

                support_phone_number_regex = re.compile(
                    r"^[+]*[-\s\./0-9]*[(]{0,1}[0-9]{1,4}[)]{0,1}[-\s\./0-9]*$")
                support_phone_processed = support_phone_number_regex.search(
                    elem)

                if support_email_processed:
                    support_email = support_email_processed.group().strip()

                if support_phone_processed:
                    support_phone = support_phone_processed.group().strip()

        dev_partners_href = response.xpath(
            "((//ul[@class='vc-app-listing-support-section__list'])[3]//li[@class='vc-app-listing-support-section__list-item'])[1]//@href").get()

        for elem in about_developer_section_list:
            if 'average rating' in elem:
                dev_average_rating = elem.strip()

            if 'building apps for the Shopify App Store' in elem:
                dev_experience = elem.strip()

            if 'website' in elem.lower():
                website_exists_boolean = True

        if website_exists_boolean:
            all_hrefs_in_about_dev_tab = response.xpath(
                "((//ul[@class='vc-app-listing-support-section__list'])[3]//li[@class='vc-app-listing-support-section__list-item'])//@href").getall()

            dev_website = all_hrefs_in_about_dev_tab[-1]

        if dev_partners_href is not None:
            dev_id = hashlib.md5(
                dev_partners_href.lower().encode()).hexdigest()

        yield Request(url=dev_partners_href, callback=self.parse_apps_associated_with_developer,
                      meta={'dev_id': dev_id, 'dev_support_email': support_email, 'dev_support_number': support_phone,
                            'dev_average_rating': dev_average_rating, 'dev_partners_href': dev_partners_href,
                            'dev_experience': dev_experience, 'dev_website': dev_website})

    def parse_apps_associated_with_developer(self, response):

        dev_id = response.meta.get('dev_id')
        support_email = response.meta.get('dev_support_email')
        support_phone = response.meta.get('dev_support_number')
        dev_average_rating = response.meta.get('dev_average_rating')
        dev_partners_href = response.meta.get('dev_partners_href')
        dev_experience = response.meta.get('dev_experience')
        dev_website = response.meta.get('dev_website')

        developed_apps_list = []

        developed_apps_href = response.xpath(
            "//div[@class='grid__item grid__item--tablet-up-half grid__item--desktop-up-quarter grid-item--app-card-listing']//a//@href").getall()
        for app_href in developed_apps_href:
            app_id = hashlib.md5(app_href.lower().encode()).hexdigest()
            developed_apps_list.append(app_id)

        yield Developer(
            dev_id=dev_id,
            dev_support_email=support_email,
            dev_support_number=support_phone,
            dev_average_rating=dev_average_rating,
            dev_partners_href=dev_partners_href,
            dev_experience=dev_experience,
            dev_website=dev_website
        )

        for developed_app in developed_apps_list:
            yield DevelopedAppsMediator(dev_id=dev_id, developed_app=developed_app)

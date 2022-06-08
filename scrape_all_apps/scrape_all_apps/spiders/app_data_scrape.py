# scrapy crawl app_data_scrape --output "feed/%(name)s-%(time)s.json" --output-format json

from ..items import App, AffinityAppMediator
import scrapy
import re
import uuid
import hashlib

from scrapy import Request


class AppDataScrapeSpider(scrapy.spiders.SitemapSpider):
    APP_PAGE_REGEX = r"https://apps.shopify.com/((\w+)(-\w+)*)$"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'scrape_all_apps'

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

        image_url = response.xpath(
            "(//div[@class='vc-app-listing-about-section__title']//img//@src)[1]").get()

        app_name = response.xpath(
            "//h1[@class='vc-app-listing-hero__heading']//text()").get().strip()

        app_intro_vid_url = response.xpath(
            "//div[@class='vc-app-listing-hero__media vc-app-listing-hero__media--video']//iframe//@src").get()

        app_developer_url = response.xpath(
            "//div[@class='vc-app-listing-hero__by-line']//a//@href").get()

        app_brief_description = response.xpath(
            "//p[@class='vc-app-listing-hero__tagline']//text()").get().strip()

        app_full_description_list = response.xpath(
            "(//div[@class='block vc-app-listing-about-section__description'])[1]//text()").getall()

        app_full_description = ""
        for description_sentence_idx in range(1, len(app_full_description_list)-1):
            description_sentence = app_full_description_list[description_sentence_idx]
            if description_sentence != "\n":
                app_full_description = app_full_description + \
                    (str(description_sentence).strip().strip() + '\n')

        app_rating = response.xpath(
            "(//div[@class='popover-wrapper js-popover']//span[@class='ui-star-rating__rating']//text())[1]").get()

        num_of_reviews_regex = re.compile(r'\d+')
        view_all_reviews_string = response.xpath(
            "//div[@class='new-app-listing-reviews-summary__view-all-link']//span//text()").get()

        if view_all_reviews_string is None:
            app_num_of_reviews = 0

        elif view_all_reviews_string == 'View all reviews':
            app_num_of_reviews = 1

        else:
            view_all_reviews_string = view_all_reviews_string.strip().replace(",", "")
            app_num_of_reviews_res = num_of_reviews_regex.search(
                view_all_reviews_string)
            app_num_of_reviews = (
                app_num_of_reviews_res.group()).strip().replace(",", "")

        app_pricing_string = response.xpath(
            "//div[@class='ui-app-pricing ui-app-pricing--format-detail']//text()").get()

        if app_pricing_string is not None:
            app_pricing_string = app_pricing_string.strip()

        free_trial = response.xpath(
            "//span[@class='app-listing-title__sub-heading']//text()").get()
        price_packages_tier_name = response.xpath(
            "//div[@class='pricing-plan-card__title']//p//text()").getall()
        price_packages = response.xpath(
            "//div[@class='pricing-plan-card__title']//h3[@class='pricing-plan-card__title-header']//text()").getall()

        price_packages = None
        price_packaes_details_list = None

        price_packages = response.xpath(
            "//h3[@class='pricing-plan-card__title-header']").getall()

        num_of_price_plans = len(price_packages)

        date_published = response.xpath(
            "(//div[@class='block__content']//span[@class='vc-app-listing-about-section__published-date__text'])[1]//text()").get().strip()

        integrations_list = response.xpath(
            "(//div[@class='block__content']//ul[@class='vc-app-listing-about-section__integrations-list'])[1]//li[@class='vc-app-listing-about-section__integrations-list__item']//text()").getall()

        integrations_list = [x.strip().replace("'", "\\'")
                             for x in integrations_list]

        illustration_images_href = response.xpath(
            "//ul[@class='vc-gallery-component__items']//img//@src").get()

        illustration_images_href = illustration_images_href.strip()

        affinity_apps_href_list = response.xpath(
            "//div[@class='vc-app-listing-similar-apps__item']//a//@href").getall()
        affinity_apps_id_list = []

        app_href_regex = re.compile(r"https://apps.shopify.com/((\w+)(-\w+)*)")

        for app_href in affinity_apps_href_list:
            app_href_processed = app_href_regex.search(app_href)
            app_href_final = app_href_processed.group()
            app_id = hashlib.md5(app_href_final.lower().encode()).hexdigest()

            affinity_apps_id_list.append(app_id)

        app_url = response.url
        app_id = hashlib.md5(app_url.lower().encode()).hexdigest()

        categories_list = []
        for category in response.css('.vc-app-listing-hero__taxonomy-links a::text').extract():
            category_id = hashlib.md5(category.lower().encode()).hexdigest()
            categories_list.append(category_id)

        if app_name is not None:
            app_name = app_name.replace("'", "\\'")

        if app_brief_description is not None:
            app_brief_description = app_brief_description.replace("'", "\\'")

        if app_full_description is not None:
            app_full_description = app_full_description.replace("'", "\\'")

        if app_pricing_string is not None:
            app_pricing_string = app_pricing_string.replace("'", "\\'")

        # performed ".replace("'", "\\'")" for all elements in list.

        yield App(
            app_id=app_id,
            app_logo=image_url,
            app_title=app_name,
            app_intro_vid_url=app_intro_vid_url,
            app_developer_link=app_developer_url,
            app_illustration_image=illustration_images_href,
            app_brief_description=app_brief_description,
            app_full_description=app_full_description,
            app_rating=app_rating,
            app_num_of_reviews=app_num_of_reviews,
            app_pricing_hint=app_pricing_string,
            app_url=app_url,
            app_published_date=date_published,
            app_integrated_apps=integrations_list,

        )

        for affinity_app_id in affinity_apps_id_list:
            yield AffinityAppMediator(
                app_id=app_id,
                affinity_app_id=affinity_app_id,
            )

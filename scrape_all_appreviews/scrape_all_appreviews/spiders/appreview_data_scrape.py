# scrapy crawl appreview_data_scrape --output "feed/%(name)s-%(time)s.json" --output-format json

from ..items import AppReview
from bs4 import BeautifulSoup

import scrapy
import re
import hashlib

from scrapy import Request


class AppreviewDataScrapeSpider(scrapy.spiders.SitemapSpider):

    APP_PAGE_REGEX = r"https://apps.shopify.com/((\w+)(-\w+)*)$"

    BASE_DOMAIN = "apps.shopify.com"

    name = 'scrape_all_appreviews'

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

        app_url = response.url
        app_id = hashlib.md5(app_url.lower().encode()).hexdigest()

        if int(app_num_of_reviews) > 0:
            app_reviews_page = response.xpath(
                "//a[@class='marketing-button marketing-button--secondary']//@href").get()
            app_reviews_page = "https://apps.shopify.com" + app_reviews_page
            yield Request(url=app_reviews_page, callback=self.parse_app_reviews, meta={'app_id': app_id})

    def parse_app_reviews(self, response):
        app_id = response.meta.get('app_id')

        for count, review in enumerate(response.xpath("//div[@class='review-listing ']")):
            author = review.css(
                '.review-listing-header>h3 ::text').extract_first(default='').strip()
            rating = review.css(
                '.review-metadata>div:nth-child(1) .ui-star-rating::attr(data-rating)').extract_first(
                default='').strip()
            # posted_at = review.css(
            #     '.review-metadata>div:nth-child(2) .review-metadata__item-value ::text').extract_first(
            #     default='').strip()
            posted_at = response.xpath(
                f"(//div[@class='review-metadata__item-label'])[{count+1}]//text()").get().strip().replace("Edited ", "")
            body = BeautifulSoup(review.css(
                '.review-content div').extract_first(), features='lxml').get_text().strip()
            helpful_count = review.css(
                '.review-helpfulness .review-helpfulness__helpful-count ::text').extract_first()
            developer_reply = BeautifulSoup(
                review.css(
                    '.review-reply .review-content div').extract_first(default=''),
                features='lxml').get_text().strip()
            developer_reply_date = review.css(
                '.review-reply div.review-reply__header-item ::text').extract_first(default='').strip()

            if developer_reply == '':
                developer_reply = None
                developer_reply_date = None
            else:
                developer_reply = developer_reply.replace("'", "\\'")

            if body == '':
                body = None

            else:
                body = body.replace("'", "\\'")

            if author == '':
                author = None

            else:
                author = author.replace("'", "\\'")

            yield AppReview(
                app_id=app_id,
                author=author,
                rating=rating,
                posted_at=posted_at,
                body=body,
                helpful_count=helpful_count,
                developer_reply=developer_reply,
                developer_reply_date=developer_reply_date
            )

        next_page_path = response.css(
            'a.search-pagination__next-page-text::attr(href)').extract_first()
        if next_page_path:
            yield Request(f"https://{self.BASE_DOMAIN}{next_page_path}", callback=self.parse_app_reviews,
                          meta={'app_id': response.meta.get('app_id')})

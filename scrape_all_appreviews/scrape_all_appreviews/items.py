# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AppReview(scrapy.Item):
    app_id = scrapy.Field()
    author = scrapy.Field()
    rating = scrapy.Field()
    posted_at = scrapy.Field()
    body = scrapy.Field()
    helpful_count = scrapy.Field()
    developer_reply = scrapy.Field()
    developer_reply_date = scrapy.Field()

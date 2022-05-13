# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class Collection(scrapy.Item):
    collection_id = scrapy.Field()
    collection_title = scrapy.Field()
    collection_description = scrapy.Field()

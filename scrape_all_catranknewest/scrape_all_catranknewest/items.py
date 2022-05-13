# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CategoryRankingNewest(scrapy.Item):
    category_id = scrapy.Field()
    app_id_list = scrapy.Field()

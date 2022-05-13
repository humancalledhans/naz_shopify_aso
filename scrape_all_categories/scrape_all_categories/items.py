import scrapy

class Category(scrapy.Item):
    category_id = scrapy.Field()
    category_title = scrapy.Field()
    category_description = scrapy.Field()

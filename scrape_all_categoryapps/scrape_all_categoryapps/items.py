import scrapy


class CategoryApp(scrapy.Item):
    category_id = scrapy.Field()
    app_id = scrapy.Field()

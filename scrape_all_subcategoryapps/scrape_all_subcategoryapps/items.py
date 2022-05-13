import scrapy

class SubCategoryApp(scrapy.Item):
    subcategory_id = scrapy.Field()
    app_id = scrapy.Field()
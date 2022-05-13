import scrapy

class SubCategory(scrapy.Item):
    parent_category_name = scrapy.Field()
    parent_category_id = scrapy.Field()
    subcategory_name = scrapy.Field()
    subcategory_id = scrapy.Field()
    subcategory_description = scrapy.Field()
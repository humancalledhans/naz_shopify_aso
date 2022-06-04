import scrapy

class SubCategoryRankingInstalled(scrapy.Item):
    subcategory_id = scrapy.Field()
    rank = scrapy.Field()
    app_id = scrapy.Field()
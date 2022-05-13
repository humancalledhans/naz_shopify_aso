import scrapy

class CategoryRankingInstalled(scrapy.Item):
    category_id = scrapy.Field()
    app_id_list = scrapy.Field()
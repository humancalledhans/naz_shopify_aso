import scrapy

class CategoryRankingInstalled(scrapy.Item):
    category_id = scrapy.Field()
    app_id = scrapy.Field()
    ranking = scrapy.Field()
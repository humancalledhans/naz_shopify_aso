import scrapy


class CategoryRankingRelevance(scrapy.Item):
    category_id = scrapy.Field()
    app_id_list = scrapy.Field()

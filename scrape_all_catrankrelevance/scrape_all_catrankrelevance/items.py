import scrapy


class CategoryRankingRelevance(scrapy.Item):
    category_id = scrapy.Field()
    ranking = scrapy.Field()
    app_id = scrapy.Field()

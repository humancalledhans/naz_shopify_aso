import scrapy

class CollectionRankingRelevance(scrapy.Item):
    collection_id = scrapy.Field()
    ranking = scrapy.Field()
    app_id = scrapy.Field()

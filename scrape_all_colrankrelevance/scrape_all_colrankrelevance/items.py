import scrapy

class CollectionRankingRelevance(scrapy.Item):
    collection_id = scrapy.Field()
    app_id_list = scrapy.Field()

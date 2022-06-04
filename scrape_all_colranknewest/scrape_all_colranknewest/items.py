import scrapy

class CollectionRankingNewest(scrapy.Item):
    collection_id = scrapy.Field()
    ranking = scrapy.Field()
    app_id = scrapy.Field()

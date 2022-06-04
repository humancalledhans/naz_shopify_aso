import scrapy

class CollectionRankingInstalled(scrapy.Item):
    collection_id = scrapy.Field()
    ranking = scrapy.Field()
    app_id = scrapy.Field()

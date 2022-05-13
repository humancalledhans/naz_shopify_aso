import scrapy

class CollectionRankingInstalled(scrapy.Item):
    collection_id = scrapy.Field()
    app_id_list = scrapy.Field()

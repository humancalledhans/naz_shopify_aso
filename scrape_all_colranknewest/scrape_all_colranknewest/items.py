import scrapy

class CollectionRankingNewest(scrapy.Item):
    collection_id = scrapy.Field()
    app_id_list = scrapy.Field()

import scrapy


class SubCategoryRankingRelevance(scrapy.Item):
    subcategory_id = scrapy.Field()
    app_id_list = scrapy.Field()
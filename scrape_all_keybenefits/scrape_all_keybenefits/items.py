import scrapy

class KeyBenefit(scrapy.Item):
    app_id = scrapy.Field()
    title = scrapy.Field()
    benefit_description = scrapy.Field()
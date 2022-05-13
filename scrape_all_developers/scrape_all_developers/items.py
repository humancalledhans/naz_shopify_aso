import scrapy

class Developer(scrapy.Item):
    dev_id = scrapy.Field()
    dev_support_email = scrapy.Field()
    dev_support_number = scrapy.Field()
    dev_average_rating = scrapy.Field()
    dev_partners_href = scrapy.Field()
    dev_experience = scrapy.Field()
    dev_website = scrapy.Field()
    developed_apps = scrapy.Field()

import scrapy


class PricingPlan(scrapy.Item):
    pricing_plan_id = scrapy.Field()
    app_id = scrapy.Field()
    pricing_plan_title = scrapy.Field()
    price = scrapy.Field()

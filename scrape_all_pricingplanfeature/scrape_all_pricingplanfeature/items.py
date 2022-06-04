import scrapy


class PricingPlanFeature(scrapy.Item):
    pricing_plan_id = scrapy.Field()
    app_id = scrapy.Field()
    feature_description = scrapy.Field()

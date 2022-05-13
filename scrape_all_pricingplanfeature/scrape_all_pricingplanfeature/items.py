import scrapy


class PricingPlanFeature(scrapy.Item):
    app_id = scrapy.Field()
    pricing_plan_id = scrapy.Field()
    feature_description = scrapy.Field()
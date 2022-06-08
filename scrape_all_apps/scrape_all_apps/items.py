import scrapy


class App(scrapy.Item):
    app_id = scrapy.Field()
    app_logo = scrapy.Field()
    app_title = scrapy.Field()
    app_intro_vid_url = scrapy.Field()
    app_developer_link = scrapy.Field()
    app_illustration_image = scrapy.Field()
    app_brief_description = scrapy.Field()
    app_full_description = scrapy.Field()
    app_rating = scrapy.Field()
    app_num_of_reviews = scrapy.Field()
    app_pricing_hint = scrapy.Field()
    app_url = scrapy.Field()
    app_published_date = scrapy.Field()
    app_integrated_apps = scrapy.Field()


class AffinityAppMediator(scrapy.Item):
    app_id = scrapy.Field()
    affinity_app_id = scrapy.Field()

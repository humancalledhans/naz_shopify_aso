import csv
from .items import PricingPlanFeature


class ScrapeAllPricingplanfeaturePipeline:
    def process_item(self, item, spider):
        return item

class ReturnInCSV(object):
    OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

    def open_spider(self, spider):
        self.write_file_headers()

    def process_item(self, item, spider):
        if isinstance(item, PricingPlanFeature):
            self.store_pricing_plan_feature(item)
            # return "PricingPlanFeature objects are now stored in CSV File."
            return item

        return item

    def write_file_headers(self):

        self.write_header("pricing_plan_features.csv",
            ['app_id', 'pricing_plan_id', 'feature_description']
            )

        return


    def store_pricing_plan_feature(self, pricing_plan_feature):
        self.write_to_out('pricing_plan_features.csv', pricing_plan_feature)
        return pricing_plan_feature


    def write_to_out(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(dict(row).values())

    def write_header(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(row)

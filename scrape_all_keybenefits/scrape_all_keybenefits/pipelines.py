import csv
from .items import KeyBenefit


class ScrapeAllKeybenefitsPipeline:
    def process_item(self, item, spider):
        return item

class ReturnInCSV(object):
    OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

    def open_spider(self, spider):
        self.write_file_headers()

    def process_item(self, item, spider):

        if isinstance(item, KeyBenefit):
            self.store_key_benefit(item)
            # return "KeyBenefit objects are now stored in CSV File."
            return item

        return item

    def write_file_headers(self):

        self.write_header("key_benefits.csv",
            ['app_id', 'title', 'description']
            )

        return


    def store_key_benefit(self, key_benefit):
        self.write_to_out('key_benefits.csv', key_benefit)
        return key_benefit


    def write_to_out(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(dict(row).values())

    def write_header(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(row)

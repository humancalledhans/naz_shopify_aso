import csv
from .items import SubCategoryApp


class ScrapeAllSubcategoryappsPipeline:
    def process_item(self, item, spider):
        return item

class ReturnInCSV(object):
    OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/"

    def open_spider(self, spider):
        self.write_file_headers()

    def process_item(self, item, spider):
        if isinstance(item, SubCategoryApp):
            self.store_subcategory_app(item)
            # return "CategoryApp objects are now stored in CSV File."
            return item

        return item


    def write_file_headers(self):
        self.write_header("subcategory_apps.csv",
            ['subcategory_id', 'app_id']
            )

        return


    def store_subcategory_app(self, subcategory_app):
        self.write_to_out('subcategory_apps.csv', subcategory_app)
        return subcategory_app


    def write_to_out(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(dict(row).values())

    def write_header(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(row)

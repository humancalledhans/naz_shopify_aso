import csv
from .items import Developer


class ScrapeAllDevelopersPipeline:
    def process_item(self, item, spider):
        return item

class ReturnInCSV(object):
    OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

    def open_spider(self, spider):
        self.write_file_headers()

    def process_item(self, item, spider):

        if isinstance(item, Developer):
            self.store_developer(item)
            # return "Developer objects are now stored in CSV File."
            return item


        return item

    def write_file_headers(self):
 
        self.write_header("developer.csv",
            ['developer_id', 'support_email', 'support_number', 'average_rating', 'shopify_link', 'experience', 'website', 'developed_apps']
            )


        return


    def store_developer(self, developer):
        self.write_to_out('developer.csv', developer)
        return developer


    def write_to_out(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(dict(row).values())

    def write_header(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(row)

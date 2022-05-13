# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
from .items import SubCategoryRankingInstalled


class ScrapeAllSubcatrankinstalledPipeline:
    def process_item(self, item, spider):
        return item

class ReturnInCSV(object):
    OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

    def open_spider(self, spider):
        self.write_file_headers()

    def process_item(self, item, spider):
        if isinstance(item, CollectionRankingInstalled):
            self.store_collectionrankinginstalled(item)
            return item

        return item

    def write_file_headers(self):
        self.write_header("subcategory_ranking_installed.csv",
            ['subcategory_id', 'app_id_list']
            )

        return


    def store_subcategoryrankinginstalled(self, subcategory_ranking_installed):
        self.write_to_out('subcategory_ranking_installed.csv', subcategory_ranking_installed)
        return subcategory_ranking_installed


    def write_to_out(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(dict(row).values())

    def write_header(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(row)

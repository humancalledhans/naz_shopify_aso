import csv
from .items import CollectionRankingNewest


class ScrapeAllColranknewestPipeline:
    def process_item(self, item, spider):
        return item

class ReturnInCSV(object):
    OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

    def open_spider(self, spider):
        self.write_file_headers()

    def process_item(self, item, spider):
 
        if isinstance(item, CollectionRankingNewest):
            self.store_collectionrankingnewest(item)
            return item

        return item

    def write_file_headers(self):

        self.write_header("collections_ranking_newest.csv",
            ['collection_id', 'app_ranking_list']
            )

        return


    def store_collectionrankingnewest(self, collection_ranking_newest):
        self.write_to_out('collections_ranking_newest.csv', collection_ranking_newest)
        return collection_ranking_newest


    def write_to_out(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(dict(row).values())

    def write_header(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(row)

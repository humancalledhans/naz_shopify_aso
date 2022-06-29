# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import mysql.connector
from datetime import datetime
from .items import SubCategoryRankingNewest


class ScrapeAllSubcatranknewestPipeline:
    def process_item(self, item, spider):
        if isinstance(item, SubCategoryRankingNewest):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, subcatranknewest_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS subcat_rank_newest(
            subcat_id VARCHAR(65535) NOT NULL,
            rank INT NOT NULL,
            app_id VARCHAR(65535) NOT NULL,
            date_time_scraped DATE NOT NULL
        );
        """

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in subcatranknewest_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in subcatranknewest_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        subcategory_id_index = columns.index('subcategory_id')
        subcategory_id = values[subcategory_id_index]

        rank_index = columns.index('rank')
        rank = values[rank_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        date_time_scraped = datetime.now()

        values = (f"{subcategory_id}, {rank}, {app_id}, {date_time_scraped}")

        insert_stmt = """
            INSERT INTO subcat_rank_newest ( subcat_id, rank, app_id, date_time_scraped ) VALUES ( %s, %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.


# class ReturnInCSV(object):
#     OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

#     def open_spider(self, spider):
#         self.write_file_headers()

#     def process_item(self, item, spider):
#         if isinstance(item, SubCategoryRankingNewest):
#             self.store_collectionrankingnewest(item)
#             return item

#         return item

#     def write_file_headers(self):
#         self.write_header("subcategory_ranking_newest.csv",
#                           ['subcategory_id', 'app_id_list']
#                           )

#         return

#     def store_subcategoryrankingnewest(self, subcategory_ranking_newest):
#         self.write_to_out('subcategory_ranking_newest.csv',
#                           subcategory_ranking_newest)
#         return subcategory_ranking_newest

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

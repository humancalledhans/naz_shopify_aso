import mysql.connector
import csv
from .items import CategoryRankingInstalled
from datetime import datetime
from .db_secrets import get_db_password

class ScrapeAllCatrankinstalledPipeline:
    def process_item(self, item, spider):
        if isinstance(item, CategoryRankingInstalled):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def upload_to_db(self, cat_rank_installed_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS cat_rank_installed(
            cat_id VARCHAR(255) NOT NULL,
            ranking INT NOT NULL,
            app_id VARCHAR(255) NOT NULL,
            date_time_scraped TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );"""
        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in cat_rank_installed_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in cat_rank_installed_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        cat_id_index = columns.index('category_id')
        cat_id = values[cat_id_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        ranking_index = columns.index('ranking')
        ranking = values[ranking_index]

        values = (app_id, cat_id, ranking)

        insert_stmt = """
            INSERT INTO cat_rank_installed (app_id, cat_id, ranking) VALUES ( %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.


# class ReturnInCSV(object):
#     OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"
#     FILE_NAME = "category_ranking_installed_latest_return_yield_return.csv"

#     def open_spider(self, spider):
#         self.write_file_headers()

#     def process_item(self, item, spider):
#         if isinstance(item, CategoryRankingInstalled):
#             self.store_categoryrankinginstalled(item)
#             return item

#         return item

#     def write_file_headers(self):
#         self.write_header(self.FILE_NAME,
#                           ['cat_id', 'rank', 'app_id']
#                           )
#         return

#     def store_categoryrankinginstalled(self, category_ranking_installed):
#         self.write_to_out(self.FILE_NAME,
#                           category_ranking_installed)
#         return category_ranking_installed

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

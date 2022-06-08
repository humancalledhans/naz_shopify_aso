import csv
import mysql.connector
from .items import CategoryRankingNewest
from datetime import datetime
# NOT DONE. 3th June


class ScrapeAllCatrankinstalledPipeline:
    def process_item(self, item, spider):
        if isinstance(item, CategoryRankingNewest):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def upload_to_db(self, cat_rank_installed_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS cat_rank_newest(
            cat_id VARCHAR(65535) NOT NULL,
            rank INT NOT NULL,
            app_id VARCHAR(65535) NOT NULL,
            date_time_scraped DATE
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

        rank_index = columns.index('rank')
        rank = values[rank_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        date_time_scraped = datetime.now()

        values = (f"{cat_id}, {rank}, {app_id}, {date_time_scraped}")
        insert_stmt = """
            INSERT INTO cat_rank_newest ( cat_id, rank, app_id, date_time_scraped ) VALUES ( %s, %s, %s, %s )
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
#         if isinstance(item, CategoryRankingNewest):
#             self.store_categoryrankingnewest(item)
#             return item

#         return item


#     def write_file_headers(self):
#         self.write_header("category_ranking_newest.csv",
#             ['category_id', 'app_id_list']
#             )

#         return

#     def store_categoryrankingnewest(self, category_ranking_newest):
#         self.write_to_out('category_ranking_newest.csv', category_ranking_newest)
#         return category_ranking_installed


#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

import csv
import mysql.connector
from .items import CategoryApp
from .db_secrets import get_db_password

class ScrapeAllCategoryappsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, CategoryApp):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def upload_to_db(self, categoryapp_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS category_app(
            category_id VARCHAR(65535) NOT NULL,
            app_id VARCHAR(65535) NOT NULL
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in categoryapp_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in categoryapp_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        category_id_index = columns.index('category_id')
        category_id = values[category_id_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        values = (category_id, app_id)

        insert_stmt = """
            REPLACE INTO category_app ( category_id, app_id ) VALUES ( %s, %s )
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
#         if isinstance(item, CategoryApp):
#             self.store_category_app(item)
#             # return "CategoryApp objects are now stored in CSV File."
#             return item

#         return item

#     def write_file_headers(self):

#         self.write_header("category_apps.csv",
#                           ['category_id', 'app_id']
#                           )

#         return

#     def store_category_app(self, category_app):
#         self.write_to_out('category_apps.csv', category_app)
#         return category_app

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

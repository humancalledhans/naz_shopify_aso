import csv
import mysql.connector
from .items import SubCategoryApp
from .db_secrets import get_db_password

class ScrapeAllSubcategoryappsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, SubCategoryApp):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, subcategoryapps_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS subcategory_app(
            subcategory_id VARCHAR(65535),
            app_id VARCHAR(65535)
        );
        """

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in subcategoryapps_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in subcategoryapps_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        subcategory_id_index = columns.index('subcategory_id')
        subcategory_id = values[subcategory_id_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        values = (subcategory_id, app_id)

        insert_stmt = """
            REPLACE INTO subcategory_app ( subcategory_id, app_id ) VALUES ( %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

# class ReturnInCSV(object):
#     OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/"

#     def open_spider(self, spider):
#         self.write_file_headers()

#     def process_item(self, item, spider):
#         if isinstance(item, SubCategoryApp):
#             self.store_subcategory_app(item)
#             # return "CategoryApp objects are now stored in CSV File."
#             return item

#         return item

#     def write_file_headers(self):
#         self.write_header("subcategory_apps.csv",
#                           ['subcategory_id', 'app_id']
#                           )

#         return

#     def store_subcategory_app(self, subcategory_app):
#         self.write_to_out('subcategory_apps.csv', subcategory_app)
#         return subcategory_app

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

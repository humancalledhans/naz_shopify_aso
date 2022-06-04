import csv
import mysql.connector
from .items import SubCategory


class ScrapeAllSubcategoriesPipeline:
    def process_item(self, item, spider):
        if isinstance(item, SubCategory):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def upload_to_db(self, subcategory_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS subcategory(
            subcategory_id VARCHAR(255) PRIMARY KEY,
            subcategory_description VARCHAR(65535),
            subcategory_name VARCHAR(65535),
            parent_category_id VARCHAR(65535),
            parent_category_name VARCHAR(65535)
            
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in subcategory_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in subcategory_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        subcategory_id_index = columns.index('subcategory_id')
        subcategory_id = values[subcategory_id_index]

        subcategory_description_index = columns.index(
            'subcategory_description')
        subcategory_description = values[subcategory_description_index]

        subcategory_name_index = columns.index('subcategory_name')
        subcategory_name = values[subcategory_name_index]

        parent_category_id_index = columns.index('parent_category_name')
        parent_category_id = values[parent_category_id_index]

        parent_category_name_index = columns.index('parent_category_name')
        parent_category_name = values[parent_category_name_index]

        values = (
            f"{subcategory_id}, {subcategory_description}, {subcategory_name}, {parent_category_id}, {parent_category_name}")

        insert_stmt = """
            REPLACE INTO subcategory ( subcategory_id, subcategory_description, subcategory_name, parent_category_id, parent_category_name ) VALUES ( %s, %s, %s, %s, %s )
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

#         if isinstance(item, SubCategory):
#             self.store_subcategory(item)
#             return item

#         return item

#     def write_file_headers(self):

#         self.write_header("subcategories.csv",
#                           ['parent_category_name', 'parent_category_id',
#                               'subcategory_name', 'subcategory_id', 'subcategory_description']
#                           )

#         return

#     def store_subcategory(self, subcategory):
#         self.write_to_out('subcategories.csv', subcategory)
#         return subcategory

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

# python3 launcher.py works. data saved in db.
import mysql.connector
from .items import Category
from .db_secrets import get_db_password

class ScrapeAllCategoriesPipeline:
    def process_item(self, item, spider):
        if isinstance(item, Category):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def upload_to_db(self, category_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS category(
            category_id VARCHAR(65535) NOT NULL,
            category_title VARCHAR(65535) NOT NULL,
            category_description VARCHAR(65535)
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in category_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in category_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        category_id_index = columns.index('category_id')
        category_id = values[category_id_index]

        category_title_index = columns.index('category_title')
        category_title = values[category_title_index]

        category_description_index = columns.index('category_description')
        category_description = values[category_description_index]

        values = (category_id, category_title, category_description)

        insert_stmt = """
            REPLACE INTO category ( category_id, category_title, category_description ) VALUES ( %s, %s, %s )
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
#         if isinstance(item, Category):
#             self.store_category(item)
#             return item

#         return item


#     def write_file_headers(self):
#         self.write_header("categories.csv",
#             ['id', 'category_title', 'category_description']
#             )

#         return


#     def store_category(self, category):
#         self.write_to_out('categories.csv', category)
#         return category


#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

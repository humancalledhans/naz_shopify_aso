# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
from .items import Collection
# from itemadapter import ItemAdapter


class ScrapeAllCollectionsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, Collection):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item


    def upload_to_db(self, collection_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022', 
            host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS collection(
            collection_id VARCHAR(255) PRIMARY KEY,
            collection_title VARCHAR(65535) NOT NULL,
            collection_description VARCHAR(65535) NOT NULL
        );"""


        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_') for x in collection_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_') for x in collection_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        collection_id_index = columns.index('collection_id')
        collection_id = values[collection_id_index]

        collection_title_index = columns.index('collection_title')
        collection_title = values[collection_title_index]

        collection_description_index = columns.index('collection_description')
        collection_description = values[collection_description_index]

        values = (f"{collection_id}, {collection_title}, {collection_description}")

        insert_stmt = """
            REPLACE INTO collection ( collection_id, collection_title, collection_description ) VALUES ( %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close() # closing the connection.

# class ReturnInCSV(object):
#     OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

#     def open_spider(self, spider):
#         self.write_file_headers()

#     def process_item(self, item, spider):
#         if isinstance(item, Collection):
#             self.store_collection(item)
#             return item

#         return item

#     def write_file_headers(self):
#         self.write_header("collections.csv",
#             ['collection_id', 'collection_title', 'collection_description']
#             )

#         return


#     def store_collection(self, collection):
#         self.write_to_out('collections.csv', collection)
#         return collection

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import mysql.connector
from .items import AppReview
# from itemadapter import ItemAdapter


class ScrapeAllAppreviewsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, AppReview):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def upload_to_db(self, appreview_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS app_review(
            app_review_id INT PRIMARY KEY AUTO_INCREMENT,
            app_id VARCHAR(65535) NOT NULL,
            author VARCHAR(65535) NOT NULL,
            rating VARCHAR(65535),
            posted_at VARCHAR(65535),
            body VARCHAR(65535) NOT NULL,
            helpful_count VARCHAR(65535),
            developer_reply VARCHAR(65535),
            developer_reply_date VARCHAR(65535)
                
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x)
                                        for x in appreview_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x)
                                       for x in appreview_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        insert_stmt = """
            REPLACE INTO app_review ( app_id, author, rating, posted_at, body, helpful_count, developer_reply, developer_reply_date ) 
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )
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
#         if isinstance(item, App):
#             self.store_app(item)
#             # return "Apps are now stored in CSV File."
#             return item

#         if isinstance(item, AppReview):
#             self.store_app_review(item)
#             # return "AppReview objects are now stored in CSV File."
#             return item

#         return item

#     def write_file_headers(self):

#         self.write_header("reviews.csv",
#             ['app_id', 'author', 'rating', 'posted_at', 'body',
#             'helpful_count', 'developer_reply', 'developer_reply_date']
#             )

#         return


#     def store_app_review(self, app_review):
#         self.write_to_out('reviews.csv', app_review)
#         return app_review


#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

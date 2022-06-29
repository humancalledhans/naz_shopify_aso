import csv
import mysql.connector
from datetime import datetime
from .items import KeyBenefit


class ScrapeAllKeybenefitsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, KeyBenefit):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, keybenefit_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS key_benefit(
            app_id VARCHAR(65535) NOT NULL,
            title VARCHAR(65535) NOT NULL,
            benefit_description VARCHAR(65535),
            scraped_date_time DATE
        );
        """

        columns = 'AaT3C~*~GA@PQT'.join(str(x)
                                        for x in keybenefit_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x)
                                       for x in keybenefit_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        title_index = columns.index('title')
        title = values[title_index]

        benefit_description_index = columns.index('benefit_description')
        benefit_description = values[benefit_description_index]

        scraped_date_time = datetime.now()

        values = (app_id, title, benefit_description, scraped_date_time)
        insert_stmt = """
        INSERT INTO key_benefit ( app_id, title, benefit_description, scraped_date_time ) 
        VALUES ( %s, %s, %s, %s )
        """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()


# class ReturnInCSV(object):
#     OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

#     def open_spider(self, spider):
#         self.write_file_headers()

#     def process_item(self, item, spider):

#         if isinstance(item, KeyBenefit):
#             self.store_key_benefit(item)
#             # return "KeyBenefit objects are now stored in CSV File."
#             return item

#         return item

#     def write_file_headers(self):

#         self.write_header("key_benefits.csv",
#                           ['app_id', 'title', 'description']
#                           )

#         return

#     def store_key_benefit(self, key_benefit):
#         self.write_to_out('key_benefits.csv', key_benefit)
#         return key_benefit

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

import csv
from .items import Developer


class ScrapeAllDevelopersPipeline:
   def process_item(self, item, spider):
        if isinstance(item, Developer):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item


    def upload_to_db(self, developer_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022', 
            host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS developer(
            dev_id VARCHAR(255) PRIMARY KEY,
            dev_support_email VARCHAR(25535),
            dev_support_number VARCHAR(25535),
            dev_average_rating DECIMAL(2,2),
            dev_partners_href VARCHAR(25535),
            dev_experience VARCHAR(25535),
            dev_website VARCHAR(25535)
        );"""


        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_') for x in developer_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_') for x in developer.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        dev_id_index = columns.index('dev_id')
        dev_id = values[dev_id_index]

        dev_support_email_index = columns.index('dev_support_email')
        dev_support_email = values[dev_support_email_index]

        dev_support_number_index = columns.index('dev_support_number')
        dev_support_number = values[dev_support_number_index]

        dev_average_rating_index = columns.index('dev_average_rating')
        dev_average_rating = values[dev_average_rating_index]

        dev_partners_href_index = columns.index('dev_partners_href')
        dev_partners_href = values[dev_partners_href_index]

        dev_experience_index = columns.index('dev_experience')
        dev_experience = values[dev_experience_index]

        dev_website_index = columns.index('dev_website')
        dev_website = values[dev_website]

        developed_apps_index = columns.index('developed_apps')
        developed_apps = values[developed_apps_index]

        values = (f"{dev_id}, {dev_support_email}, {dev_support_number}, \
            {dev_average_rating}, {dev_partners_href}, {dev_experience}, \
            {dev_website}") ###TODO: DEVELOPED_APPS WONT WORK!

        insert_stmt = """
            INSERT INTO col_rank_relevance_data ( collection_id, ranking, app_id ) VALUES ( %s, %s, %s )
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

#         if isinstance(item, Developer):
#             self.store_developer(item)
#             # return "Developer objects are now stored in CSV File."
#             return item


#         return item

#     def write_file_headers(self):
 
#         self.write_header("developer.csv",
#             ['developer_id', 'support_email', 'support_number', 'average_rating', 'shopify_link', 'experience', 'website', 'developed_apps']
#             )


#         return


#     def store_developer(self, developer):
#         self.write_to_out('developer.csv', developer)
#         return developer


#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

import csv
import mysql.connector
from .items import PricingPlanFeature
from .db_secrets import get_db_password

class ScrapeAllPricingplanfeaturePipeline:
    def process_item(self, item, spider):
        if isinstance(item, PricingPlanFeature):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, pricingplanfeature_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS pricing_plan_feature (
            pricing_plan_id VARCHAR(65535) NOT NULL,
            app_id VARCHAR(65535) NOT NULL,
            feature_description VARCHAR(65535) NOT NULL,
            date_time_scraped TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x)
                                        for x in pricingplanfeature_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x)
                                       for x in pricingplanfeature_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        pricing_plan_id_index = columns.index('pricing_plan_id')
        pricing_plan_id = values[pricing_plan_id_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        feature_description_index = columns.index('feature_description')
        feature_description = values[feature_description_index]

        values = (pricing_plan_id, app_id, feature_description)

        insert_stmt = """
            REPLACE INTO pricing_plan_feature ( pricing_plan_id, app_id, feature_description ) 
            VALUES ( %s, %s, %s )
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
#         if isinstance(item, PricingPlanFeature):
#             self.store_pricing_plan_feature(item)
#             # return "PricingPlanFeature objects are now stored in CSV File."
#             return item

#         return item

#     def write_file_headers(self):

#         self.write_header("pricing_plan_features.csv",
#                           ['app_id', 'pricing_plan_id', 'feature_description']
#                           )

#         return

#     def store_pricing_plan_feature(self, pricing_plan_feature):
#         self.write_to_out('pricing_plan_features.csv', pricing_plan_feature)
#         return pricing_plan_feature

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

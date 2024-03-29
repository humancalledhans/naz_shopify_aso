import mysql.connector
from .items import PricingPlan
from .db_secrets import get_db_password

class ScrapeAllPricingplansPipeline:
    def process_item(self, item, spider):
        if isinstance(item, PricingPlan):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def upload_to_db(self, pricingplan_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS pricing_plan(
            pricing_plan_id VARCHAR(65535) NOT NULL,
            app_id VARCHAR(65535) NOT NULL,
            pricing_plan_title VARCHAR(65535) NOT NULL,
            price VARCHAR(65535) NOT NULL,
            date_time_scraped TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x)
                                        for x in pricingplan_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x)
                                       for x in pricingplan_data.values())

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

        pricing_plan_title_index = columns.index('pricing_plan_title')
        pricing_plan_title = values[pricing_plan_title_index]

        price_index = columns.index('price')
        price = values[price_index]

        values = (pricing_plan_id, app_id, pricing_plan_title, price)

        insert_stmt = """
            INSERT INTO pricing_plan ( pricing_plan_id, app_id, pricing_plan_title, price ) 
            VALUES ( %s, %s, %s, %s )
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

#         if isinstance(item, PricingPlan):
#             self.store_pricing_plan(item)
#             # return "PricingPlan obejcts are now stored in CSV File."
#             return item

#         return item

    # def write_file_headers(self):
    #     self.write_header("pricing_plans.csv",
    #         ['pricing_plan_id', 'app_id', 'pricing_plan_title', 'price']
    #         )

    #     return

    # def store_pricing_plan(self, pricing_plan):
    #     self.write_to_out('pricing_plans.csv', pricing_plan)
    #     return pricing_plan

    # def write_to_out(self, file_name, row):
    #     with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
    #         csv_output = csv.writer(output)
    #         csv_output.writerow(dict(row).values())

    # def write_header(self, file_name, row):
    #     with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
    #         csv_output = csv.writer(output)
    #         csv_output.writerow(row)

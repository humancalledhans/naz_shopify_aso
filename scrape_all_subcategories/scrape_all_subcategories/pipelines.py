import csv
from .items import SubCategory


class ScrapeAllSubcategoriesPipeline:
    def process_item(self, item, spider):
        if isinstance(item, SubCategory):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item


    def upload_to_db(self, appreview_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022', 
            host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()
        add_appreview = ("INSERT INTO app_review"
            "review_id, app_id, author, rating, posted_at, body, helpful_count, developer_reply, developer_reply_date)"
            "VALUES %(review_id)s, %(app_id)s, %(author)s, %(rating)s, %(posted_at)s, %(body)s, %(helpful_count)s, %(developer_reply)s, %(developer_reply_date)s ")

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS app_review(
            parent_category_name VARCHAR(65535) NOT NULL,
            parent_category_id VARCHAR(65535) NOT NULL,
            subcategory_name VARCHAR(65535),
            subcategory_id VARCHAR(65535) NOT NULL,
            subcategory_description VARCHAR(65535)
        );"""


        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_') for x in appreview_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_') for x in appreview_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        insert_stmt = """
            REPLACE INTO app_review ( app_id, author, rating, posted_at, body, helpful_count, developer_reply, developer_reply_date ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close() # closing the connection.

class ReturnInCSV(object):
    OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

    def open_spider(self, spider):
        self.write_file_headers()

    def process_item(self, item, spider):

        if isinstance(item, SubCategory):
            self.store_subcategory(item)
            return item

        return item


    def write_file_headers(self):

        self.write_header("subcategories.csv", 
            ['parent_category_name', 'parent_category_id', 'subcategory_name', 'subcategory_id', 'subcategory_description']
            )

        return


    def store_subcategory(self, subcategory):
        self.write_to_out('subcategories.csv', subcategory)
        return subcategory


    def write_to_out(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(dict(row).values())

    def write_header(self, file_name, row):
        with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
            csv_output = csv.writer(output)
            csv_output.writerow(row)

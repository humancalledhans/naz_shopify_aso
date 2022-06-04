import csv
from .items import CollectionRankingRelevance
from datetime import datetime

class ScrapeAllColrankrelevancePipeline:
   def process_item(self, item, spider):
        if isinstance(item, CollectionRankingRelevance):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item


    def upload_to_db(self, col_rank_relevance_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022', 
            host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS col_rank_relevance(
            col_id VARCHAR(25535) NOT NULL,
            rank INT NOT NULL,
            app_id VARCHAR(25535) NOT NULL
        );"""


        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_') for x in col_rank_relevance_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_') for x in col_rank_relevance_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        collection_id_index = columns.index('collection_id')
        collection_id = values[collection_id_index]

        rank_index = columns.index('ranking')
        ranking = values[rank_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        date_time_scraped = datetime.now()

        values = (f"{collection_id}, {ranking}, {app_id}, {date_time_scraped}")

        insert_stmt = """
            INSERT INTO col_rank_relevance_data ( collection_id, ranking, app_id, date_time_scraped ) VALUES ( %s, %s, %s, %s )
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
 
#         if isinstance(item, CollectionRankingRelevance):
#             self.store_collectionrankingrelevance(item)
#             return item

#         return item

#     def write_file_headers(self):

#         self.write_header("collections_ranking_relevance.csv",
#             ['collection_id', 'app_ranking_list']
#             )

#         return


#     def store_collectionrankingrelevance(self, collection_ranking_relevance):
#         self.write_to_out('collections_ranking_relevance.csv', collection_ranking_relevance)
#         return collection_ranking_relevance


#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

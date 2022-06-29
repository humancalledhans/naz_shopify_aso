import mysql.connector
from .items import CollectionRankingNewest


class ScrapeAllColranknewestPipeline:
    def process_item(self, item, spider):
        if isinstance(item, CollectionRankingNewest):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, col_rank_newest_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS col_rank_newest(
            col_id VARCHAR(25535) NOT NULL,
            ranking INT NOT NULL,
            app_id VARCHAR(25535) NOT NULL,
            date_time_scraped TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in col_rank_newest_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in col_rank_newest_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        collection_id_index = columns.index('collection_id')
        collection_id = values[collection_id_index]

        rank_index = columns.index('ranking')
        ranking = values[rank_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        values = (collection_id, ranking, app_id)

        insert_stmt = """
            INSERT INTO col_rank_newest ( col_id, ranking, app_id ) VALUES ( %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

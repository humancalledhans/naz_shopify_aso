import mysql.connector
from .items import CategoryRankingNewest
from datetime import datetime


class ScrapeAllCatranknewestPipeline:
    def process_item(self, item, spider):
        if isinstance(item, CategoryRankingNewest):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, cat_rank_installed_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')

        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS cat_rank_newest(
            cat_id VARCHAR(65535) NOT NULL,
            ranking INT(5),
            app_id VARCHAR(65535) NOT NULL,
            date_time_scraped TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in cat_rank_installed_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in cat_rank_installed_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        cat_id_index = columns.index('category_id')
        cat_id = values[cat_id_index]

        rank_index = columns.index('rank')
        rank = values[rank_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        cat_rank_values = (cat_id, rank, app_id)

        insert_stmt = "INSERT INTO cat_rank_newest ( cat_id, ranking, app_id ) VALUES ( %s, %s, %s );"

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, cat_rank_values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

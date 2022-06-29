import mysql.connector
from datetime import datetime
from .items import SubCategoryRankingInstalled


class ScrapeAllSubcatrankinstalledPipeline:
    def process_item(self, item, spider):
        if isinstance(item, SubCategoryRankingInstalled):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, subcatrankinstalled_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS subcat_rank_installed(
            subcat_id VARCHAR(65535) NOT NULL,
            rank INT NOT NULL,
            app_id VARCHAR(65535) NOT NULL,
            date_time_scraped DATE NOT NULL
        );
        """

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in subcatrankinstalled_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in subcatrankinstalled_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        subcategory_id_index = columns.index('subcategory_id')
        subcategory_id = values[subcategory_id_index]

        rank_index = columns.index('rank')
        rank = values[rank_index]

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        date_time_scraped = datetime.now()

        values = (f"{subcategory_id}, {rank}, {app_id}, {date_time_scraped}")

        insert_stmt = """
            INSERT INTO subcat_rank_installed ( subcat_id, rank, app_id, date_time_scraped ) VALUES ( %s, %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

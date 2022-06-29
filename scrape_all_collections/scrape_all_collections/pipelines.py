import mysql.connector
from .items import Collection

# collection scraper works.


class ScrapeAllCollectionsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, Collection):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, collection_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS collection(
            collection_id VARCHAR(255) PRIMARY KEY,
            collection_title VARCHAR(65535) NOT NULL,
            collection_description VARCHAR(65535) NOT NULL
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in collection_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in collection_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        collection_id_index = columns.index('collection_id')
        collection_id = values[collection_id_index]

        collection_title_index = columns.index('collection_title')
        collection_title = values[collection_title_index]

        collection_description_index = columns.index('collection_description')
        collection_description = values[collection_description_index]

        values = (collection_id, collection_title, collection_description)

        insert_stmt = """
            REPLACE INTO collection ( collection_id, collection_title, collection_description ) VALUES ( %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

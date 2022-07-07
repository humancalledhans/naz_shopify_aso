import mysql.connector
from .items import Developer, DevelopedAppsMediator
from .db_secrets import get_db_password

class ScrapeAllDevelopersPipeline:
    def process_item(self, item, spider):
        if isinstance(item, Developer):
            self.upload_to_db(item)
            return item
        elif isinstance(item, DevelopedAppsMediator):
            self.upload_mediator_to_db(item)
            return item

    def upload_to_db(self, developer_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
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

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in developer_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in developer_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

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
        dev_website = values[dev_website_index]

        values = (dev_id, dev_support_email, dev_support_number,
                  dev_average_rating, dev_partners_href, dev_experience, dev_website)

        insert_stmt = """
            REPLACE INTO developer ( dev_id, dev_support_email, dev_support_number, dev_average_rating, dev_partners_href, dev_experience, dev_website ) VALUES ( %s, %s, %s, %s, %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

    def upload_mediator_to_db(self, mediator_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='sys')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS developed_apps_mediator(
            dev_id VARCHAR(255) PRIMARY KEY,
            developed_app VARCHAR(25535)
        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x).replace('/', '_')
                                        for x in mediator_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x).replace('/', '_')
                                       for x in mediator_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        dev_id_index = columns.index('dev_id')
        dev_id = values[dev_id_index]

        developed_app_index = columns.index('developed_app')
        developed_app = values[developed_app_index]

        values = (dev_id, developed_app)

        insert_stmt = """
        REPLACE INTO developed_apps_mediator ( dev_id, developed_app ) VALUES ( %s, %s )
        """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)
        cnx.commit()

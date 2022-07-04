# pushed to docker on aws.
# python3 launcher.py works. data saved in db.

from .items import AffinityAppMediator, App

import mysql.connector
from datetime import datetime


class ScrapeAllAppsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, App):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item
        elif isinstance(item, AffinityAppMediator):
            self.upload_mediator_to_db(item)
            return item

    def upload_to_db(self, app_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS app(
            app_id VARCHAR(255) PRIMARY KEY,
            app_logo VARCHAR(65535) NOT NULL,
            app_title VARCHAR(65535),
            app_intro_vid_url VARCHAR(65535),
            app_developer_link VARCHAR(65535) NOT NULL,
            app_illustration_image VARCHAR(65535),
            app_brief_description VARCHAR(65535),
            app_full_description VARCHAR(65535),
            app_rating VARCHAR(65535),
            app_num_of_reviews VARCHAR(65535),
            app_pricing_hint VARCHAR(65535),
            app_url VARCHAR(65535),
            app_published_date DATE NOT NULL,
            scraped_date_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x)
                                        for x in app_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x)
                                       for x in app_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        app_logo_index = columns.index('app_logo')
        app_logo = values[app_logo_index]

        app_title_index = columns.index('app_title')
        app_title = values[app_title_index]

        app_intro_vid_url_index = columns.index('app_intro_vid_url')
        app_intro_vid_url = values[app_intro_vid_url_index]
        if app_intro_vid_url == 'None':
            app_intro_vid_url = None

        app_developer_link_index = columns.index('app_developer_link')
        app_developer_link = values[app_developer_link_index]

        app_illustration_image_index = columns.index('app_illustration_image')
        app_illustration_image = values[app_illustration_image_index]

        app_brief_description_index = columns.index('app_brief_description')
        app_brief_description = values[app_brief_description_index]

        app_full_description_index = columns.index('app_full_description')
        app_full_description = values[app_full_description_index]

        app_rating_index = columns.index('app_rating')
        app_rating = values[app_rating_index]

        app_num_of_reviews_index = columns.index('app_num_of_reviews')
        app_num_of_reviews = values[app_num_of_reviews_index]

        app_pricing_hint_index = columns.index('app_pricing_hint')
        app_pricing_hint = values[app_pricing_hint_index]

        app_url_index = columns.index('app_url')
        app_url = values[app_url_index]

        app_published_date_index = columns.index('app_published_date')
        date_string = values[app_published_date_index]
        app_published_date = datetime.strptime(date_string, "%B %d, %Y")

        app_values = (app_id, app_logo, app_title, app_intro_vid_url, app_developer_link, app_illustration_image, app_brief_description,
                      app_full_description, app_rating, app_num_of_reviews, app_pricing_hint, app_url, app_published_date)

        insert_stmt = """
            REPLACE INTO app ( app_id, app_logo, app_title, app_intro_vid_url, app_developer_link, app_illustration_image, app_brief_description, app_full_description, app_rating, app_num_of_reviews, app_pricing_hint, app_url, app_published_date )
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
            """

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        # print("INSERT_STMTT: ", insert_stmt)
        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, app_values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

    def upload_mediator_to_db(self, mediator_data):

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS affinity_apps_mediator(
            parent_app_id VARCHAR(65535) NOT NULL,
            affinity_app_id VARCHAR(65535) NOT NULL
        );"""

        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        columns = 'AaT3C~*~GA@PQT'.join(str(x)
                                        for x in mediator_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x)
                                       for x in mediator_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        affinity_app_id_index = columns.index('affinity_app_id')
        affinity_app_id = values[affinity_app_id_index]

        values = (app_id, affinity_app_id)
        insert_stmt = """
        REPLACE INTO affinity_apps_mediator ( parent_app_id, affinity_app_id ) VALUES ( %s, %s )
        """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)
        cnx.commit()

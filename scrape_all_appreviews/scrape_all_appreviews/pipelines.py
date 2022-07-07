# appreviews scraper works.

import hashlib
import mysql.connector
from .items import AppReview
from datetime import datetime
from .db_secrets import get_db_password

class ScrapeAllAppreviewsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, AppReview):
            self.upload_to_db(item)
            return item

    def upload_to_db(self, appreview_data):
        cnx = mysql.connector.connect(user='admin', password=get_db_password(),
                                      host='shopify-aso-free-tier.c200z18i1oar.us-east-1.rds.amazonaws.com', database='db_shopify_aso')
        cursor = cnx.cursor()

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS app_review(
            app_review_id VARCHAR(255) PRIMARY KEY,
            app_id VARCHAR(65535) NOT NULL,
            author VARCHAR(65535) NOT NULL,
            rating INT(11) NOT NULL,
            posted_at DATE,
            body VARCHAR(65535) NOT NULL,
            helpful_count VARCHAR(65535),
            developer_reply VARCHAR(65535),
            developer_reply_date DATE

        );"""

        columns = 'AaT3C~*~GA@PQT'.join(str(x)
                                        for x in appreview_data.keys())
        values = 'AaT7C~*~GA@PQT'.join(str(x)
                                       for x in appreview_data.values())

        columns = tuple(map(str, columns.split('AaT3C~*~GA@PQT')))
        values = tuple(map(str, values.split('AaT7C~*~GA@PQT')))
        # print("COLUMNS AFTER MAPPING ", columns)
        # print("VALUES AFTER MAPPING", values)

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        app_id_index = columns.index('app_id')
        app_id = values[app_id_index]

        author_index = columns.index('author')
        author = values[author_index]

        rating_index = columns.index('rating')
        rating = int(values[rating_index])

        posted_at_index = columns.index('posted_at')
        posted_at = values[posted_at_index]

        body_index = columns.index('body')
        body = values[body_index]

        helpful_count_index = columns.index('helpful_count')
        helpful_count = values[helpful_count_index]

        developer_reply_index = columns.index('developer_reply')
        developer_reply = values[developer_reply_index]

        developer_reply_date_index = columns.index('developer_reply_date')
        developer_reply_date = values[developer_reply_date_index]

        app_review_id = hashlib.md5(
            (body+app_id).lower().encode()).hexdigest()
        # so that we replace the same app reviews whenever we scrape again. like, when the developer adds a reply, the same app review is replaced, instead of inserting a new entry.

        if posted_at == '':
            posted_at = None
        elif posted_at is None:
            posted_at = None
        elif posted_at == 'None':
            posted_at = None
        else:
            posted_at = datetime.strptime(posted_at, "%B %d, %Y")

            print("POSTED_AT:", posted_at)

        if developer_reply_date == '':
            developer_reply_date = None
        elif developer_reply_date is None:
            developer_reply_date = None
        elif developer_reply_date == 'None':
            developer_reply_date = None
        else:
            developer_reply_date = datetime.strptime(
                developer_reply_date, "%B %d, %Y")

        values = (app_review_id, app_id, author, rating, posted_at, body,
                  helpful_count, developer_reply, developer_reply_date)

        insert_stmt = """
            REPLACE INTO app_review ( app_review_id, app_id, author, rating, posted_at, body, helpful_count, developer_reply, developer_reply_date )
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )
            """

        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt, values)

        cnx.commit()
        cursor.close()
        cnx.close()  # closing the connection.

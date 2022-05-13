import csv
from .items import App

import mysql.connector
from datetime import datetime
import re
import sys
import boto3
import os


class ScrapeAllAppsPipeline:
    def process_item(self, item, spider):
        if isinstance(item, App):
            self.upload_to_db(item)
            # return "Apps are now stored in CSV File."
            return item

    def add_app(self, cursor, cnx, app_data):

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
            app_published_date VARCHAR(65535)
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

        affinity_apps_list_index = columns.index('affinity_apps_id_list')
        affinity_apps_id_list = columns[affinity_apps_list_index]

        self.add_affinity_app_mediator(
            cursor=cursor, cnx=cnx, parent_app_id=app_id, affinity_apps_id_list=affinity_apps_id_list)

        insert_stmt = f"""
            REPLACE INTO app ( app_id, app_logo, app_title, app_intro_vid_url, app_developer_link, app_illustration_image, app_brief_description, app_full_description, app_rating, app_num_of_reviews, app_pricing_hint, app_url, app_published_date )
            VALUES ('{app_id}', '{app_logo}', '{app_title}', '{app_intro_vid_url}', '{app_developer_link}', '{app_illustration_image}', '{app_brief_description}', '{app_full_description}', '{app_rating}', '{app_num_of_reviews}', '{app_pricing_hint}', '{app_url}', '{app_published_date}')
            """

        # print("LEN_OF_COLUMNS", len(columns))
        # print("LEN_OF_VALUES", len(values))

        # print("INSERT_STMTT: ", insert_stmt)
        cursor.execute(create_table_statement)
        cursor.execute(insert_stmt)

        # cnx.commit()

    def add_affinity_app_mediator(self, cursor, cnx, parent_app_id, affinity_apps_id_list):

        create_table_statement = """
        CREATE TABLE IF NOT EXISTS affinity_apps_mediator(
            affinity_apps_mediator_id VARCHAR(65535) PRIMARY KEY,
            parent_app_id VARCHAR(65535) NOT NULL,
            affinity_app_id VARCHAR(65535) NOT NULL
        );"""

        for affinity_app_id in affinity_apps_id_list:
            values = f"({parent_app_id},{affinity_app_id})"
            insert_stmt = """
            REPLACE INTO affinity_apps_mediator ( parent_app_id, affinity_app_id ) VALUES ( %s, %s )
            """

            # cursor.execute(create_table_statement)
            # cursor.execute(insert_stmt, values)
            # cnx.commit()

    def upload_to_db(self, app_data):
        cnx = mysql.connector.connect(user='admin', password='pa$$w0RD2022',
                                      host='naz-shopify-aso-db.cluster-c200z18i1oar.us-east-1.rds.amazonaws.com', database='naz_shopify_aso_DB')
        cursor = cnx.cursor()

        self.add_app(cursor=cursor, cnx=cnx, app_data=app_data)

        # add_app

        cursor.close()
        cnx.close()  # closing the connection.


# class ReturnInCSV(object):
#     OUTPUT_DIRECTORY = "/Users/hans/Desktop/Files/Non-Monash/Business/Working/2022/Main/Naz - Dev Apps/scraper_csv_files/AWS-Tester/"

#     def open_spider(self, spider):
#         self.write_file_headers()

#     def process_item(self, item, spider):
#         if isinstance(item, App):
#             self.store_app(item)
#             # return "Apps are now stored in CSV File."
#             return item

#         return item

#     def write_file_headers(self):
#         self.write_header("apps.csv",
#                           ['app_id', 'app_logo', 'app_title', 'app_intro_vid_url', 'app_developer_link', 'app_illustration_images',
#                            'app_brief_description', 'app_full_description',
#                            'app_rating', 'app_num_of_reviews', 'app_pricing_hint', 'app_url', 'app_published_date',
#                            'app_integrated_apps', 'affinity_apps_id_list', 'app_category_id_list']
#                           )

#         return

#     def store_app(self, app):
#         self.write_to_out('apps.csv', app)
#         return app

#     def write_to_out(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(dict(row).values())

#     def write_header(self, file_name, row):
#         with open(f"{self.OUTPUT_DIRECTORY}{file_name}", 'a', encoding='utf-8') as output:
#             csv_output = csv.writer(output)
#             csv_output.writerow(row)

FROM python:3.9

WORKDIR /usr/src/app

COPY ./scrape_all_subcategoryapps ./scrape_all_subcategoryapps
COPY ./scrapy.cfg ./
COPY ./launcher.py ./
COPY requirements.txt ./

RUN pip3 install -r requirements.txt --target .
RUN pip3 install scrapy
RUN pip3 install beautifulsoup4
RUN pip3 install scrapy-user-agents
RUN pip3 install webscrapingapi-scrapy-sdk
RUN pip3 install boto3
RUN pip3 install mysql-connector

CMD ["python3", "launcher.py"]

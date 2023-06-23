import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, BinaryType


class WebUtils:

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('web_scrape', self.scrape, returnType=BinaryType())
        spark.udf.register('web_scrape_text', self.scrape_text, returnType=StringType())

    @staticmethod
    def scrape(url: str):
        response = requests.get(url)

        return response.content

    @staticmethod
    def scrape_text(url: str):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text(' ')

        return text

import sys
import os
import certifi
from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging
from NYC_Taxi_Traffic.constant import *
import pymongo

ca = certifi.where()

class MongoDBClient:

    client=None

    def __init__(self,database_name=DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGO_DB_URL_KEY)
                if mongo_db_url is None:
                    raise Exception(f"Enviroment key {MONGO_DB_URL_KEY} is not set")
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAfile=ca)
                self.client = MongoDBClient.client
                self.database = self.client[database_name]
                self.database_name = database_name

                logging.info("MongoDB Connection Successfull")

        except Exception as e:
            raise CustomException(e,sys)
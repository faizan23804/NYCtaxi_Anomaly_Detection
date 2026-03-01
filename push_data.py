import os, json, sys 
import certifi  # type: ignore
import pymongo  # type: ignore
import pandas as pd # type: ignore
import numpy as np # type: ignore
from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging

from dotenv import load_dotenv # type: ignore

load_dotenv()

MONGO_DB_URL = os.getenv("NycDB_URL")
print(MONGO_DB_URL)

ca = certifi.where()

class NetworkDataUpload:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise CustomException(e,sys)
        
    
    def csv_to_json(self,file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True,inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise CustomException(e,sys)


    def insert_data_to_MongoDB(self,records,database,collection):
        try:
            self.records=records
            self.database=database
            self.collection=collection

            self.mongoclient = pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongoclient[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)

            logging.info("Conversion from Dataframe to Json Succesfull")

            return(len(self.records))
        except Exception as e:
            raise CustomException(e,sys)
        

if __name__ == '__main__':
    try:
        FILE_PATH="notebooks\TaxiNYC.csv"
        Database="NYC_Taxi_Traffic"
        Collection="TrafficDataset"
        networkobj=NetworkDataUpload()
        records=networkobj.csv_to_json(file_path=FILE_PATH)
        #print(records)
        number_of_records=networkobj.insert_data_to_MongoDB(records,Database,Collection)
        print(f"The Total Number of Records are: {number_of_records}")
        logging.info("Dataset Pushed to Mongo Database.")
    except Exception as e:
        raise CustomException(e,sys)


from NYC_Taxi_Traffic.configurations.mongodb_conn import *
from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging
from NYC_Taxi_Traffic.constant import *
import pandas as pd
import numpy as np
import sys
from typing import Optional

class NYC():

    def __init__(self) -> None:

        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise CustomException(e,sys)
        
    def extract_collection_as_dataframe(self,collection_name: str, database_name:Optional[str]=None):

        try:
            if database_name is None:
                collection = self.mongo_client.database[collection_name]

            df = pd.DataFrame(list(collection.find()))
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            if 'Unnamed: 0' in df.columns:
                df = df.drop('Unnamed: 0', axis=1)
            if 'value' in df.columns:
                df = df.rename(columns={"value": "passengers"})

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            df.set_index('timestamp', inplace=True)
            
            df.replace({'na':np.nan},inplace=True)
            return df
        except Exception as e:
            raise CustomException(e,sys)
        

    def add_features(self, df: pd.DataFrame) -> pd.DataFrame:

        """
        Adds 4 new time series features:
        - hour
        - day_of_week
        - rolling_mean
        - rolling_std
        """

        try:
            
            df['hour'] = df.index.hour # type: ignore

            df["day_of_week"] = df.index.dayofweek # type: ignore

            df["rolling_mean"] = df['passengers'].rolling(window=ROLLING_WINDOW).mean()

            df["rolling_std"] = df['passengers'].rolling(window=ROLLING_WINDOW).std()

            #dropping null values
            df.dropna(inplace=True)

            logging.info(f"New Features added. Columns: {list(df.columns)}")
            return df

        except Exception as e:
            raise CustomException(e,sys)

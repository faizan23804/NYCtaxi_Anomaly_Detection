import os
import sys

from pandas import DataFrame

from NYC_Taxi_Traffic.entity.config_entity import DataIngestionConfig
from NYC_Taxi_Traffic.entity.artifact_entity import DataIngestionArtifact
from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging
from NYC_Taxi_Traffic.data_access.extract_n_fe import NYC



class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig=DataIngestionConfig()):
        """
        :param data_ingestion_config: configuration for data ingestion
        """
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise CustomException(e,sys)
        

    
    def export_data_into_feature_store(self)->DataFrame:
        """
        Method Name :   export_data_into_feature_store
        Description :   This method exports data from mongodb to csv file
        
        Output      :   data is returned as artifact of data ingestion components
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            logging.info(f"Exporting data from mongodb")
            nyc_data = NYC()
            dataframe = nyc_data.extract_collection_as_dataframe(collection_name= self.data_ingestion_config.collection_name)
            logging.info(f"Shape of dataframe: {dataframe.shape}")
            feature_store_file_path  = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            logging.info(f"Saving exported data into feature store file path: {feature_store_file_path}")

            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe

        except Exception as e:
            raise CustomException(e,sys)
        


        


    
    def initiate_data_ingestion(self) ->DataIngestionArtifact:
        """
        Method Name :   initiate_data_ingestion
        Description :   This method initiates the data ingestion components of training pipeline 
        
        Output      :   Dataset is returned as the artifact of data ingestion component
        On Failure  :   Write an exception log and then raise an exception
        """
        logging.info("Entered initiate_data_ingestion method of Data_Ingestion class")

        try:
            dataframe = self.export_data_into_feature_store()

            logging.info("Got the data from mongodb")

            logging.info(
                "Exited initiate_data_ingestion method of Data_Ingestion class"
            )

            data_ingestion_artifact = DataIngestionArtifact(feature_store_file_path=self.data_ingestion_config.feature_store_file_path)
            
            logging.info(f"Data ingestion artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise CustomException(e, sys) from e
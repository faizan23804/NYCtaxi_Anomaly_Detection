import os,sys
import numpy as np
import pandas as pd
import pickle
from sklearn.preprocessing import StandardScaler

from NYC_Taxi_Traffic.entity.config_entity import DataTransformationConfig
from NYC_Taxi_Traffic.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact, DataTransformationArtifact
from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging
from NYC_Taxi_Traffic.utils.main_utils import save_object, save_numpy_array_data



class DataTransformation:

    def __init__(self,data_ingestion_artifact: DataIngestionArtifact, 
                 data_validation_artifact:DataValidationArtifact,
                  data_transformation_config: DataTransformationConfig ):
        
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config

        except Exception as e:
            raise CustomException(e,sys)
        

    
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise CustomException(e,sys)
        
    @staticmethod
    def get_scaler() -> StandardScaler:
        try:
            return StandardScaler()
        except Exception as e:
            raise CustomException(e,sys)
    

    
    def initiate_data_transformation(self) -> DataTransformationArtifact:
        logging.info("Entered initiate_data_transformation method of DataTransformation class")
            
        try:
            df = self.read_data(file_path=self.data_ingestion_artifact.feature_store_file_path)

            logging.info(f"Dataset loaded for transformation. Shape: {df.shape}")

            scaler = self.get_scaler()
            scaled_array = scaler.fit_transform(df)
            logging.info(f"Scaling complete. Shape: {scaled_array.shape}")

            transformed_path = save_numpy_array_data(file_path=self.data_transformation_config.data_transformation_transformed_dir,array=scaled_array)
            logging.info(f"Transformed data saved: {transformed_path}")

            preprocessor_path = save_object(file_path=self.data_transformation_config.data_trasformation_object_dir, obj = scaler)
            logging.info(f"Scaler saved: {preprocessor_path}")

            data_transformation_artifact = DataTransformationArtifact(
                transformed_file_path=transformed_path, # type: ignore
                preprocessor_object_file_path=preprocessor_path # type: ignore
            )

            logging.info(f"Data transformation artifact: {data_transformation_artifact}")
            logging.info("Exited initiate_data_transformation method of DataTransformation class")
            return data_transformation_artifact

        except Exception as e:
            raise CustomException(e,sys)
            
           
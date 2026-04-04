import os, sys
import pandas as pd
import numpy as np
import json
from pandas import DataFrame


from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging
from NYC_Taxi_Traffic.constant import SCHEMA_FILE_PATH
from NYC_Taxi_Traffic.utils.main_utils import read_yaml_file, write_yaml_file
from NYC_Taxi_Traffic.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from NYC_Taxi_Traffic.entity.config_entity import DataValidationConfig


class DataValidation:

    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):

        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise CustomException(e,sys)


    def validate_columns(self,df: pd.DataFrame):

        try:
            status = len(df.columns) == len(self.schema_config['columns'])
            logging.info(f"Are the Number of Columns Valid: {status}")
            return status
        except Exception as e:
            raise CustomException(e,sys)
        
    def validate_null_values(self, df: pd.DataFrame):
        
        try:
            null_counts = df.isnull().sum()
            cols_with_nulls = null_counts[null_counts > 0].to_dict()
            if cols_with_nulls:
                return False, logging.info(f"Null values found: {cols_with_nulls}")
             
            return True, logging.info("No null values found")
            
        except Exception as e:
            raise CustomException(e, sys)
        
    def validate_value_range(self,df: pd.DataFrame):

        try:
            ranges = self.schema_config.get("value_ranges",{})
            error = []
            for cols,limits in ranges.items():
                if cols not in df.columns:
                    continue
                if 'min' in limits and df[cols].min() < limits['min']:
                    error.append(f"{cols} has values below {limits['min']}")
                if 'max' in limits and df[cols].max() > limits['max']:
                    error.append(f"{cols} has values below {limits['max']}")

            if error:
                return False, logging.info(f"Range errors: {error}")
            
            return True, logging.info("All value ranges valid")
        
                
        except Exception as e:
            raise CustomException(e, sys)
        

    def initiate_data_validation(self) -> DataValidationArtifact:
        logging.info("Entered initiate_data_validation method of DataValidation class")

        try:
            # Load dataset
            df = pd.read_csv(self.data_ingestion_artifact.feature_store_file_path)
            logging.info(f"Dataset loaded for validation. Shape: {df.shape}")

            # Run all checks
            checks = {
                "column_validation"    : self.validate_columns(df),
                "null_value_validation": self.validate_null_values(df),
                "value_range_validation": self.validate_value_range(df),
            }

            report = {}
            overall_status = True
            for check_name, status in checks.items():
                report[check_name] = status
                logging.info(f"{check_name}: {status}")

                if not status:
                    overall_status = False

            report["overall_status"] = overall_status

            write_yaml_file(file_path=self.data_validation_config.drift_report_file_path, content=report)

            if not overall_status:
                raise Exception(f"Data Validation Failed. Check report: "
                                f"{self.data_validation_config.drift_report_file_path}")

            data_validation_artifact = DataValidationArtifact(validation_status=overall_status,
                                                              drift_report_file_path=self.data_validation_config.drift_report_file_path)

            logging.info(f"Data validation artifact: {data_validation_artifact}")
            logging.info("Exited initiate_data_validation method of DataValidation class")
            return data_validation_artifact

        except Exception as e:
            raise CustomException(e, sys)


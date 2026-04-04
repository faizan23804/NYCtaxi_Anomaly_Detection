import os, sys
import numpy as np # type: ignore

DATABASE_NAME = "NYC_Taxi_Traffic"
COLLECTION_NAME = "TrafficDataset"
MONGO_DB_URL_KEY = "NycDB_URL"

"""
Defining common constant variable for training pipeline

"""

PIPELINE_NAME:str = "nyc_anomaly"
ARTIFACTS_DIR:str = "Artifacts"
FILE_NAME:str = "taxiNYC.csv"
PREPROCESSING_OBJECT_FILE_NAME = "preprocessing.pkl"
SCALED_FILE_NAME:str = "obj.npy"
ROLLING_WINDOW: int = 48

SCHEMA_FILE_PATH = os.path.join('config','schema.yaml')

"""
Data Ingestion related constant start with Data_Ingestion VAR name.
"""
DATA_INGESTION_COLLECTION_NAME:str = "TrafficDataset"
DATA_INGESTION_DIR_NAME:str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR:str = "feature_store"
DATA_INGESTION_INGESTED_DIR:str = "ingested"


"""
Data Validation related constant start with Data_VALIDATION VAR name.
"""
DATA_VALIDATION_DIR_NAME:str = "data_validation"
DATA_VALIDATION_DRIFT_REPORT_DIR:str = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME:str = "report.yaml"


"""
Data Transformation related constant start with Data_Transformation VAR name.
"""
DATA_TRANSFORMATION_DIR_NAME:str = "data_transformation"
DATA_TRANSFORMATION_TRANSFORMED_OBJECT_DIR_NAME:str = "preprocessing_object"


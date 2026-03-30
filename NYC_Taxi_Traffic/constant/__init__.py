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
TRAIN_FILE_NAME:str = "train.csv"
TEST_FILE_NAME:str = "test.csv"
PREPROCESSING_OBJECT_FILE_NAME = "preprocessing.pkl"
SCALED_TRAIN_FILE_NAME:str = "train.npy"
SCALED_TEST_FILE_NAME:str = "test.npy"

"""
Data Ingestion related constant start with Data_Ingestion VAR name.
"""
DATA_INGESTION_COLLECTION_NAME:str = "Nyc_AnomaLy_Data"
DATA_INGESTION_DIR_NAME:str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR:str = "feature_store"
DATA_INGESTION_INGESTED_DIR:str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO:float = 0.25
import os, sys
from dataclasses import dataclass
from datetime import datetime
from NYC_Taxi_Traffic.constant import *

timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")

@dataclass
class TrainingPipelineConfig:
    pipeline_name: str = PIPELINE_NAME
    artifact_dir: str = os.path.join(ARTIFACTS_DIR,timestamp)
    timestamp: str = timestamp

training_pipeline_config : TrainingPipelineConfig = TrainingPipelineConfig()

@dataclass
class DataIngestionConfig:
    data_ingestion_dir: str = os.path.join(training_pipeline_config.artifact_dir, DATA_INGESTION_DIR_NAME)
    feature_store_file_path: str = os.path.join(data_ingestion_dir, DATA_INGESTION_FEATURE_STORE_DIR, FILE_NAME)
    collection_name: str = DATA_INGESTION_COLLECTION_NAME

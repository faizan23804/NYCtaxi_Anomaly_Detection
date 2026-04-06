import os, sys
import numpy as np # type: ignore
import pandas as pd # type: ignore
from sklearn.ensemble import IsolationForest # type: ignore

from NYC_Taxi_Traffic.entity.config_entity import ModelTrainerConfig
from NYC_Taxi_Traffic.entity.artifact_entity import (
    DataTransformationArtifact,
    ModelTrainerArtifact
)
from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging
from NYC_Taxi_Traffic.constant import *
from NYC_Taxi_Traffic.utils.main_utils import *


class ModelTrainer:
    def __init__(self, data_transformation_artifact: DataTransformationArtifact, model_trainer_config: ModelTrainerConfig):

        try:
            self.data_transformation_artifact = data_transformation_artifact
            self.model_trainer_config = model_trainer_config
        except Exception as e:
            raise CustomException(e,sys)
        
    @staticmethod
    def get_model():
        return IsolationForest(
            n_estimators=MODEL_TRAINER_N_ESTIMATORS,
            contamination=MODEL_TRAINER_CONTAMINATION,
            max_features=MODEL_TRAINER_MAX_FEATURES,
            random_state=MODEL_TRAINER_RANDOM_STATE
        )
    
    def train_model(self,data: np.ndarray):

        try:
            logging.info("Training isolation Forest moddel")
            model = self.get_model()
            model.fit(data)
            logging.info("Model training complete")
            return model
        except Exception as e:
            raise CustomException(e,sys)
        
    
    def detect_anomalies(self, model: IsolationForest, data: np.ndarray) -> tuple:
        """
        Predicts anomalies on the dataset.
        Isolation Forest returns:
            1  → normal
           -1  → anomaly
        """
        try:
            predictions = model.predict(data)           # 1 or -1 for each row
            scores = model.decision_function(data)      # anomaly score per row

            nos_anomalies = int(np.sum(predictions == -1))
            anomaly_pct = round((nos_anomalies / len(predictions)) * 100, 2)

            logging.info(f"Total records    : {len(predictions)}")
            logging.info(f"Anomalies found  : {nos_anomalies}")
            logging.info(f"Anomaly %        : {anomaly_pct}%")

            return predictions, scores, nos_anomalies, anomaly_pct
        except Exception as e:
            raise CustomException(e, sys)
        
    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        logging.info("Entered initiate_model_trainer method of ModelTrainer class")

        try:
            data = load_numpy_array_data(file_path=self.data_transformation_artifact.transformed_file_path)
            model = self.train_model(data)
            predictions, scores, nos_anomalies, anomaly_pct = self.detect_anomalies(model,data)
            save_object(file_path=self.model_trainer_config.trained_model_file_path,obj=model)

            model_trainer_artifact = ModelTrainerArtifact(
                trained_model_file_path=self.model_trainer_config.trained_model_file_path,
                n_estimators=MODEL_TRAINER_N_ESTIMATORS,
                contamination=MODEL_TRAINER_CONTAMINATION,
                n_anomalies_detected=nos_anomalies,
                anomaly_percentage=anomaly_pct
            )

            logging.info(f"Model Trainer Artifact: {model_trainer_artifact}")
            logging.info("Exited initiate_model_trainer method of ModelTrainer class")
            return model_trainer_artifact
        except Exception as e:
            raise CustomException(e,sys)
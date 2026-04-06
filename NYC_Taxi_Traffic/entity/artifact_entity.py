from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    feature_store_file_path: str


@dataclass
class DataValidationArtifact:
    validation_status: bool
    drift_report_file_path: str

@dataclass
class DataTransformationArtifact:
     transformed_file_path: str 
     preprocessor_object_file_path:str


@dataclass
class ModelTrainerArtifact:
    trained_model_file_path: str       
    n_estimators: int                   
    contamination: float                
    n_anomalies_detected: int           
    anomaly_percentage: float
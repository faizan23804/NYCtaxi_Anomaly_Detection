import yaml # type: ignore
import dill # type: ignore
import pickle
from NYC_Taxi_Traffic.exceptions.exception import CustomException
from NYC_Taxi_Traffic.logger.logging import logging
import os
import sys
import numpy as np # type: ignore
from pandas import DataFrame # type: ignore


def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path,'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise CustomException(e,sys)
    

def write_yaml_file(file_path: str, content: object, replace: bool = False):
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise CustomException(e, sys)
    

def save_object(file_path: str, obj: object):
    try:
        logging.info("Entered the save_object method of MainUtils class")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)
        logging.info("Exited the save_object method of MainUtils class")
        return file_path
    except Exception as e:
        raise CustomException(e, sys) from e

def load_object(file_path:str):
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file {file_path} does not exists")
        with open(file_path ,'rb') as file:
            print(file)
            return pickle.load(file)
    except Exception as e:
        raise CustomException(e, sys)
    

def save_numpy_array_data(file_path: str, array: np.ndarray):
   
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
        return file_path
    except Exception as e:
        raise CustomException(e, sys) from e
    
    
def load_numpy_array_data(file_path:str):
    
    try:
        with open(file_path,"rb") as obj:
            return np.load(obj)
    except Exception as e:
        raise CustomException(e, sys)
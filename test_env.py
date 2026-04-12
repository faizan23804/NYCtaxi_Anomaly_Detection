import os
from dotenv import load_dotenv # type: ignore
import pandas as pd

df = pd.read_csv(r'D:\End-to-end-ML\AnomalyDetection\Artifacts\06_04_2026_23_34_30\data_ingestion\feature_store\taxiNYC.csv')
print(df.head(10))

load_dotenv()  

print("MONGO_DB_URL =", os.getenv("NycDB_URL"))
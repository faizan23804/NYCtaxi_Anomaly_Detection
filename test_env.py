# test_env.py
import os
from dotenv import load_dotenv # type: ignore

load_dotenv()  

print("MONGO_DB_URL =", os.getenv("NycDB_URL"))
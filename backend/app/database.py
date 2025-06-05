# app/database.py
import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return oracledb.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASSWORD"),
        dsn=f"{os.getenv('ORACLE_HOST')}:{os.getenv('ORACLE_PORT')}/{os.getenv('ORACLE_SID')}"
    )

# import sys 
# sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
# from src.config import My_Config as cfg
import os
import pandas as pd


# Env-vars
BLOBNAME = os.environ.get("BLOBNAME")
CONTAINERNAME = os.environ.get("CONTAINERNAME")
ACCOUNTNAME = os.environ.get("ACCOUNTNAME")
QUERYSTRING = os.environ.get("QUERYSTRING")


def create_sas():    
    sas_url = f"https://{ACCOUNTNAME}.blob.core.windows.net/{CONTAINERNAME}/{BLOBNAME}{QUERYSTRING}"
    return sas_url


def get_data():
    sas_url = create_sas()
    df = pd.read_parquet(sas_url)
    return df

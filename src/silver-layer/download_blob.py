# import sys 
# sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
# from src.config import My_Config as cfg
import os
import pandas as pd

# needs refactoring
# Env-vars
BLOBNAME = os.environ.get("STORAGE_BLOB_NAME")
CONTAINERNAME = os.environ.get("STORAGE_CONTAINER_NAME_1")
ACCOUNTNAME = os.environ.get("STORAGE_ACCOUNT_NAME")
QUERYSTRING = os.environ.get("BLOB_QUERY_STRING")


def create_sas(blobpath):    
    sas_url = f"https://{ACCOUNTNAME}.blob.core.windows.net/{CONTAINERNAME}/{blobpath}/{BLOBNAME}{QUERYSTRING}"
    return sas_url


def get_data(blobpath):
    sas_url = create_sas(blobpath)
    df = pd.read_parquet(sas_url)
    return df

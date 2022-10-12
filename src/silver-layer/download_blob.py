import sys
sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
from src.config import My_Config as cfg
import pandas as pd



# Env-vars
LOCAL_FILE_PATH = cfg.local_files_path()
CONTAINERNAME = cfg.storage_container_name_1()
BLOBNAME = cfg.storage_blob_name()
ACCOUNTNAME = cfg.storage_account_name()
QUERYSTRING = cfg.blob_query_string()
CONNECTION_STRING = cfg.storage_connection_string()


def create_sas():    
    sas_url = f"https://{ACCOUNTNAME}.blob.core.windows.net/{CONTAINERNAME}/{BLOBNAME}{QUERYSTRING}"
    return sas_url


def get_data():
    sas_url = create_sas()
    df = pd.read_parquet(sas_url)
    return df


# get query maker from your other project and integrate into this one
# make own gcp pipeline
# figure out how to store dims and facts per customer

import sys
sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
from config_old import My_Config as cfg
import pandas as pd


def create_sas(blobname):
    # Env-vars
    CONTAINERNAME = cfg.storage_container_name_2()
    ACCOUNTNAME = cfg.storage_account_name()
    QUERYSTRING = cfg.blob_query_string_2()   
    sas_url = f"https://{ACCOUNTNAME}.blob.core.windows.net/{CONTAINERNAME}/{blobname}{QUERYSTRING}"
    return sas_url


def get_data(blobname):
    sas_url = create_sas(blobname)
    df = pd.read_csv(sas_url)
    return df
# Imports
import sys
sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
from src.config import My_Config as cfg
import pandas_profiling as pp 
import pandas as pd


CONNECTION_STRING = cfg.storage_connection_string()
CONTAINERNAME = cfg.storage_container_name_1()
LOCAL_FILE_PATH = cfg.local_files_path()
ACCOUNTNAME = cfg.storage_account_name()
QUERYSTRING = cfg.blob_query_string()


def create_sas(blob_name):
    sas_url = f"https://{ACCOUNTNAME}.blob.core.windows.net/{CONTAINERNAME}/{blob_name}{QUERYSTRING}"
    return sas_url
 
 
def get_data(sas_url):
    df = pd.read_parquet(sas_url)
    df_len, df_width = df.shape
    print('Rows: ' +str(df_len),'\nColumns: ' + str(df_width))
    return df


def clean_data(df):
    for column in df:
        x = df
        x = x[column].drop_duplicates().dropna()
        print("The column '" + column + "' has " + str(x.shape[0]) + " unique rows")


# List files from blob storage
def list_blobs_in_container():
    from azure.storage.blob import BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINERNAME)
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        blob_name = blob.name
        print(blob.name + '\n')
        sas_url = create_sas(blob_name)
        df = get_data(sas_url)
        clean_data(df)

        profile = pp.ProfileReport(df, title="Billing Report")
        profile.to_file(LOCAL_FILE_PATH + f"reports/{blob_name}.html".format())
        
    return blobs_list

list_blobs_in_container()


    



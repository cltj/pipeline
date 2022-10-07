import sys
sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
from src.config import My_Config as cfg
from download_tables import get_data
import pandas as pd


# for each table in silver blob
# if fact, store it in memory
def get_fact():
    from azure.storage.blob import BlobServiceClient, BlobBlock
    CONNECTION_STRING = cfg.storage_connection_string()
    BILLING_CONTAINER = cfg.storage_container_name_2()
    blob_service_client =  BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client=blob_service_client.get_container_client(BILLING_CONTAINER)
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)
        if "dim" not in blob.name:
            fact = get_data(blob.name)
            fact.shape
            return fact

def get_dim():
    from azure.storage.blob import BlobServiceClient, BlobBlock
    CONNECTION_STRING = cfg.storage_connection_string()
    BILLING_CONTAINER = cfg.storage_container_name_2()
    blob_service_client =  BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client=blob_service_client.get_container_client(BILLING_CONTAINER)
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)
        if "fact" not in blob.name:
            dim = get_data(blob.name)
            dim.shape
            return dim


def list_blobs():
    from azure.storage.blob import BlobServiceClient, BlobBlock
    CONNECTION_STRING = cfg.storage_connection_string()
    BILLING_CONTAINER = cfg.storage_container_name_2()
    blob_service_client =  BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client=blob_service_client.get_container_client(BILLING_CONTAINER)
    blob_list = container_client.list_blobs()
    return len(blob_list)


def combine():
    fact = get_fact()
    len_blobs = list_blobs()
    blobs = len_blobs-1
    for blob in blobs:
        dim = get_dim()
        if dim.name == blob.name:
            print(dim.name)
            # if dim name 
            # make sub table
            # make subtable unique
            # for value in subtable, (find colum in table)
            # if value in sub table and table is same
            # replace with subtable index
            

# how to write this part :/ 

#for each table in silver
# if dim table, store it
# for each index value in dim table, if col value(dim) = col vaule(fact), insert index vaule
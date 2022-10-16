import os, time


def blobs():
    from azure.storage.blob import ContainerClient
    CONTAINERNAME = os.environ.get("CONTAINERNAME")
    CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
    container_client =  ContainerClient.from_connection_string(CONNECTION_STRING, CONTAINERNAME)
    blob_list = container_client.list_blobs()
    return blob_list


def azure_billing():
    import pandas as pd
    LOCAL_FILES_PATH = os.environ.get("LOCAL_FILES_PATH")
    ACCOUNTNAME = os.environ.get("ACCOUNTNAME")
    QUERYSTRING = os.environ.get("QUERYSTRING")
    blob_list = blobs()
    df = pd.DataFrame()
    for blob in blob_list:
        container = blob.container
        blob_name = blob.name
        sas_url = f"https://{ACCOUNTNAME}.blob.core.windows.net/{container}/{blob_name}{QUERYSTRING}"
        df2 = pd.read_csv(sas_url)
        df = pd.concat([df,df2])
        
    isExist = os.path.exists(LOCAL_FILES_PATH + 'data/azure_data/')
    if isExist is False:
        os.mkdir(LOCAL_FILES_PATH + 'data/azure_data/')
        time.sleep(1)
        
    df.to_parquet(LOCAL_FILES_PATH + 'data/azure_data/Azure-Billing-Data.parquet')
    print("Azure billing data extracted!")
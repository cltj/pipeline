import boto3, os, time, pandas_gbq
from google.cloud import bigquery
from azure.storage.blob import ContainerClient
import pandas as pd
from config import Settings
settings = Settings()


def make_folder(cloud_provider):
    isExist = os.path.exists(settings.local_files_path + 'data/'+ cloud_provider)
    if isExist is False:
        os.mkdir(settings.local_files_path + 'data/' + cloud_provider)
        time.sleep(1)
    return settings.local_files_path + 'data/' + cloud_provider


def aws_billing():   
    session = boto3.Session(settings.aws_access_id, settings.aws_secret_key)
    s3 = session.resource('s3')
    bucket = s3.Bucket('collectbill')
    filenames = []
    folder = make_folder('aws_data')
    df = pd.DataFrame()
    for s3_object in bucket.objects.all():
        if s3_object.key.endswith('.parquet'):
            path, filename = os.path.split(s3_object.key)
            bucket.download_file(s3_object.key, folder+'/'+filename)
            part = pd.read_parquet(folder+'/'+filename)
            df = pd.concat([df,part], ignore_index=True)
            os.remove(folder+'/'+filename)
            filenames.append(s3_object.key)
   
    df.to_parquet(folder + '/aws-billing-data.parquet')
    print("AWS billing data extracted!")
    
    
def gcp_billing():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.gcp_credential_path
    client = bigquery.Client(settings.gcp_project_id, location=settings.gcp_dataset_location)
    dir_list = os.listdir(settings.gcp_query_filepath)
    for file in dir_list:
        if not os.path.isfile(settings.gcp_query_filepath + "/" + file):
            print('File does not exist.')
        else:
            with open(settings.gcp_query_filepath + "/" + file) as f:
                content = f.read().replace('\n',' ')
                content = content.replace("{table}", settings.gcp_gbq_table)
                query = content.replace("'{days}'", settings.gcp_extract_days)
    
        data = pandas_gbq.read_gbq(query, settings.gcp_project_id, dialect='standard')
        df = pd.DataFrame(data)
        file = file.replace('.sql','.parquet')
        folder = make_folder('gcp_data')
        df.to_parquet(folder + "/" + file, index=True)
    
    print("GCP billing data extracted!")


def azure_billing():
    container_client =  ContainerClient.from_connection_string(conn_str=settings.connection_string, container_name=settings.containername)
    blob_list = container_client.list_blobs()
    df = pd.DataFrame()
    for blob in blob_list:
        container = blob.container
        blob_name = blob.name
        sas_url = f"https://{settings.accountname}.blob.core.windows.net/{container}/{blob_name}{settings.querystring}"
        df2 = pd.read_csv(sas_url)
        df = pd.concat([df,df2])
        
    folder = make_folder('azure_data')
    df.to_parquet(folder + '/azure-billing-data.parquet')
    print("Azure billing data extracted!")
    

gcp_billing()
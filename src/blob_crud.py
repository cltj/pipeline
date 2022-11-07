from azure.storage.blob import BlobClient
import os
from config import Settings
settings = Settings()


class AzureBlobFileUploader:
    def __init__(self, container_name: str , blob_name: str):
        print("Intializing AzureBlobFileUploader")
        self.blob_client =  BlobClient.from_connection_string(
            settings.storage_connection_string,
            container_name=container_name, 
            blob_name=blob_name)
    
    def upload_file(self,file_name,path, platform):
        upload_file_path = os.path.join(path, file_name)
        print(f"uploading file - {file_name}")
        with open(upload_file_path, "rb") as data:
            self.blob_client.upload_blob(data,overwrite=True)

    def download_blob(self, blob_name: str):
        with open(settings.local_files_path + "/data/" + blob_name, "wb") as my_blob:
            blob_data = self.blob_client.download_blob()
            blob_data.readinto(my_blob)

    def list_blobs_in_container(container_name: str):
        from azure.storage.blob import ContainerClient
        container = ContainerClient.from_connection_string(
            conn_str=settings.storage_connection_string, 
            container_name=container_name)
        blob_list = container.list_blobs()
        for blob in blob_list:
            print('Downloading ' + blob.name + '\n')
            AzureBlobFileUploader().download_blob(container_name=container_name, blob_name=blob.name)


def upload_to_bronze():
    gcp_data  = os.listdir(settings.local_files_path+'data/gcp_data')
    aws_data  = os.listdir(settings.local_files_path+'data/aws_data')
    azure_data  = os.listdir(settings.local_files_path+'data/azure_data')

    for file in gcp_data:
        gcp_path = settings.local_files_path+'data/gcp_data'
        upload.upload_file(file_name=file, path=gcp_path, platform="gcp")

    for file in aws_data:
        aws_path = settings.local_files_path+'data/aws_data'
        upload.upload_file(file_name=file, path=aws_path, platform="aws")

    for file in azure_data:
        azure_path = settings.local_files_path+'data/azure_data'
        upload.upload_file(file_name=file, path=azure_path, platform="azure")

upload = AzureBlobFileUploader(settings.containername,settings.storage_container_name_1)
upload_to_bronze()
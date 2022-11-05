from azure.storage.blob import BlobServiceClient
import os, shutil
from config import Settings
settings = Settings()


def upload_parquet():
    class AzureBlobFileUploader:
        def __init__(self):
            print("Intializing AzureBlobFileUploader")
            self.blob_service_client =  BlobServiceClient.from_connection_string(settings.storage_connection_string)
        
        def upload_all_files_in_folder(self):
            gcp_data  = os.listdir(settings.local_files_path+'data/gcp_data')
            aws_data  = os.listdir(settings.local_files_path+'data/aws_data')
            azure_data  = os.listdir(settings.local_files_path+'data/azure_data')
            
            for i in gcp_data:
                gcp_path = settings.local_files_path+'data/gcp_data'
                self.upload_file(file_name=i, path=gcp_path, platform="gcp")
            
            for i in aws_data:
                aws_path = settings.local_files_path+'data/aws_data'
                self.upload_file(file_name=i, path=aws_path, platform="aws")
            
            for i in azure_data:
                azure_path = settings.local_files_path+'data/azure_data'
                self.upload_file(file_name=i, path=azure_path, platform="azure")
                
        
        def upload_file(self,file_name,path, platform):
            container_path = settings.storage_container_name_1+"/"+settings.customer_name+"/"+platform
            blob_client = self.blob_service_client.get_blob_client(container=container_path, blob=file_name) # Create blob with same name as local file name
            upload_file_path = os.path.join(path, file_name)
            
            print(f"uploading file - {file_name}")
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data,overwrite=True)

    azure_blob_file_uploader = AzureBlobFileUploader()
    azure_blob_file_uploader.upload_all_files_in_folder()
    

def cleanup_files():
    dir_path = settings.local_files_path + '/data'
    sub_dir = os.listdir(dir_path)
    for dir in sub_dir:
        try:
            shutil.rmtree(dir_path+"/"+ dir)
        except OSError as e:
            print("Error: %s : %s" % (dir_path, e.strerror))
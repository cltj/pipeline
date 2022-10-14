import os, sys
sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
from src.config import My_Config as cfg

    
def upload_parquet():
    """
    Uploads local parquet files to azure blob storage
    Params: None
    Returns: None
    """
    from azure.storage.blob import BlobServiceClient
    
    CONNECTION_STRING = cfg.storage_connection_string()
    BILLING_CONTAINER = cfg.storage_container_name_1()
    LOCAL_FILES_PATH = cfg.local_files_path()
    CUSTOMER_NAME = os.environ.get("CUSTOMER_NAME")
    
    class AzureBlobFileUploader:
        def __init__(self):
            print("Intializing AzureBlobFileUploader")
            # Initialize the connection to Azure storage account
            self.blob_service_client =  BlobServiceClient.from_connection_string(CONNECTION_STRING)
        
        def upload_all_files_in_folder(self):
            # Get all files with parquet extension and exclude directories
            directories  = os.listdir(LOCAL_FILES_PATH+'data')
            gcp_data  = os.listdir(LOCAL_FILES_PATH+'data/gcp_data')
            aws_data  = os.listdir(LOCAL_FILES_PATH+'data/aws_data')
            azure_data  = os.listdir(LOCAL_FILES_PATH+'data/azure_data')
            
            for i in gcp_data:
                gcp_path = LOCAL_FILES_PATH+'data/gcp_data'
                self.upload_file(file_name=i, path=gcp_path, platform="gcp")
            
            for i in aws_data:
                aws_path = LOCAL_FILES_PATH+'data/aws_data'
                self.upload_file(file_name=i, path=aws_path, platform="aws")
            
            for i in azure_data:
                azure_path = LOCAL_FILES_PATH+'data/azure_data'
                self.upload_file(file_name=i, path=azure_path, platform="azure")

        
        def upload_file(self,file_name,path, platform):
            container_path = BILLING_CONTAINER+"/"+CUSTOMER_NAME+"/"+platform
            blob_client = self.blob_service_client.get_blob_client(container=container_path, blob=file_name) # Create blob with same name as local file name
            upload_file_path = os.path.join(path, file_name) # This needs fixing
            
            print(f"uploading file - {file_name}")
            with open(upload_file_path, "rb") as data: # Uploads to cltj
                blob_client.upload_blob(data,overwrite=True)

    azure_blob_file_uploader = AzureBlobFileUploader() # Initialize class and upload files
    azure_blob_file_uploader.upload_all_files_in_folder()
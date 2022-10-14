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
        
        def upload_all_images_in_folder(self):
            # Get all files with parquet extension and exclude directories
            directories  = os.listdir(LOCAL_FILES_PATH+'data')
            all_file_names = []
            for dir in directories:
                dir_file_names = [f for f in os.listdir(LOCAL_FILES_PATH+'data/'+dir)
                                if os.path.isfile(os.path.join(LOCAL_FILES_PATH+'data/'+dir, f)) and ".parquet" in f]
                all_file_names.append(dir_file_names)
            
            for lst in all_file_names:    
                for file_name in lst: # Upload each file
                    self.upload_file(file_name)
        
        def upload_file(self,file_name):
            container_path = BILLING_CONTAINER+"/"+CUSTOMER_NAME+"-bronze"
            blob_client = self.blob_service_client.get_blob_client(container=container_path, blob=file_name) # Create blob with same name as local file name
            upload_file_path = os.path.join(LOCAL_FILES_PATH+'data/', file_name) # This needs fixing
            
            print(f"uploading file - {file_name}")
            with open(upload_file_path, "rb") as data: # Uploads to cltj
                blob_client.upload_blob(data,overwrite=True)

    azure_blob_file_uploader = AzureBlobFileUploader() # Initialize class and upload files
    azure_blob_file_uploader.upload_all_images_in_folder()
    
upload_parquet()
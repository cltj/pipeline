def upload_parquet():
    from azure.storage.blob import BlobServiceClient
    import os
    
    CONNECTION_STRING = os.environ.get("STORAGE_CONNECTION_STRING")
    BILLING_CONTAINER = os.environ.get("STORAGE_CONTAINER_NAME_1")
    LOCAL_FILES_PATH = os.environ.get("LOCAL_FILES_PATH")
    CUSTOMER_NAME = os.environ.get("CUSTOMER_NAME")
    
    class AzureBlobFileUploader:
        def __init__(self):
            print("Intializing AzureBlobFileUploader")
            self.blob_service_client =  BlobServiceClient.from_connection_string(CONNECTION_STRING)
        
        def upload_all_files_in_folder(self):
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
            upload_file_path = os.path.join(path, file_name)
            
            print(f"uploading file - {file_name}")
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data,overwrite=True)

    azure_blob_file_uploader = AzureBlobFileUploader()
    azure_blob_file_uploader.upload_all_files_in_folder()
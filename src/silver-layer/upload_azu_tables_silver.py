def upload_csv():
    from azure.storage.blob import BlobServiceClient
    import os
    
    # Env-vars
    LOCAL_FILE_PATH = os.environ.get("LOCAL_FILE_PATH")
    CONNECTION_STRING = os.environ.get("STORAGE_CONNECTION_STRING")
    BILLING_CONTAINER = os.environ.get("STORAGE_CONTAINER_NAME_3")
    CUSTOMER_NAME = os.environ.get("CUSTOMER_NAME")
    class AzureBlobFileUploader:
        def __init__(self):
            print("Intializing AzureBlobFileUploader")
            self.blob_service_client =  BlobServiceClient.from_connection_string(CONNECTION_STRING)
            
        def upload_all_files_in_folder(self):
            azure_data  = os.listdir(LOCAL_FILE_PATH +'split/azure')
            
            for i in azure_data:
                    azure_path = LOCAL_FILE_PATH + 'data/azure_data'
                    self.upload_file(file_name=i, path=azure_path, platform="azure")
                    
            
        def upload_file(self,file_name,path, platform):
            container_path = BILLING_CONTAINER+"/"+CUSTOMER_NAME+"/"+platform
            blob_client = self.blob_service_client.get_blob_client(container=container_path, blob=file_name)
            upload_file_path = os.path.join(path, file_name)
            
            print(f"uploading file - {file_name}")
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data,overwrite=True)


        def cleanup_files():
            split_folder = LOCAL_FILE_PATH + 'split/azure'
            for f in os.listdir(split_folder):
                if os.path.isfile(os.path.join(split_folder, f)) and f.endswith(".csv"):
                    os.remove(os.path.join(split_folder, f))
                    print("Removing local file: " + f)
                else:
                    continue
                
    
    azure_blob_file_uploader = AzureBlobFileUploader()
    azure_blob_file_uploader.upload_file()
    azure_blob_file_uploader.cleanup_files()

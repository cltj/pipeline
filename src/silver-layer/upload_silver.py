
import sys, os
sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
from src.config import My_Config as cfg
from azure.storage.blob import BlobServiceClient
from .split_data import split_data


# Env-vars
LOCAL_FILE_PATH = cfg.local_files_path()
CONTAINERNAME = cfg.storage_container_name_2()
CONNECTION_STRING = cfg.storage_connection_string()


def upload_tables():
    #create a new container
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_service_client.create_container(CONTAINERNAME)
    
    #upload the files
    split_folder = LOCAL_FILE_PATH + 'split'
    all_file_names = [f for f in os.listdir(split_folder)
                    if os.path.isfile(os.path.join(split_folder, f)) and ".csv" in f]
      
    for file_name in all_file_names:
        blob_client = blob_service_client.get_blob_client(container=CONTAINERNAME, blob=file_name)
        upload_file_path = os.path.join(split_folder, file_name)

        print(f"uploading file - {file_name}")
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)


def cleanup_files():
    """
    Cleans up local parquet files
    Params: None
    Returns: None
    """
    split_folder = LOCAL_FILE_PATH + 'split'
    for f in os.listdir(split_folder):
        if os.path.isfile(os.path.join(split_folder, f)) and f.endswith(".csv"):
            os.remove(os.path.join(split_folder, f))
            print("Removing local file: " + f)
        else:
            continue
        
        
def main():
    # create_singles_csv()
    split_data('meter','dim')
    split_data('resource','dim')
    split_data('billing','dim')
    split_data('consumption','fact')
    upload_tables()
    cleanup_files()
    print("Done!!!")


if __name__ == '__main__':
    main()

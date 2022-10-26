
import os
from azure.storage.blob import BlobServiceClient
from split_azure_data import tags_dim, sub_dim, product_dim, resource_dim, usage_fact


# Env-vars
LOCAL_FILE_PATH = os.environ.get("LOCAL_FILE_PATH")
CONNECTION_STRING = os.environ.get("STORAGE_CONNECTION_STRING")
CONTAINERNAME = os.environ.get("STORAGE_CONTAINER_NAME_2")


def upload_tables():
    #create a new container
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    # blob_service_client.create_container(container_name)
    
    #upload the files
    split_folder = LOCAL_FILE_PATH + 'split/azure'
    all_file_names = [f for f in os.listdir(split_folder)
                    if os.path.isfile(os.path.join(split_folder, f)) and ".csv" in f]
      
    for file_name in all_file_names:
        blob_client = blob_service_client.get_blob_client(container=CONTAINERNAME, blob=file_name)
        upload_file_path = os.path.join(split_folder, file_name)

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
        
        
def main():
    # create_singles_csv() # this needs reviewing
    tags_dim()
    sub_dim()
    product_dim()
    resource_dim()
    usage_fact()
    upload_tables()
    cleanup_files()
    print("Done!!!")


if __name__ == '__main__':
    main()

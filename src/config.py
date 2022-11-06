from pydantic import BaseSettings

class Settings(BaseSettings):
    local_files_path: str
    az_subscription_id: str
    az_tenant_id: str
    az_client_id: str 
    az_client_secret: str
    gcp_prices_apikey: str
    gcp_client_id: str
    gcp_client_secret: str
    gcp_gbq_table: str
    gcp_credential_path: str
    gcp_project_id: str
    gcp_dataset_location: str
    gcp_extract_days: str
    aws_secret_key: str
    aws_access_id: str
    storage_container_name_1: str
    storage_container_name_2: str
    storage_container_name_3: str
    new_container_name: str
    storage_connection_string: str
    storage_blob_name: str
    storage_account_name: str
    storage_account_key: str
    blob_query_string: str
    blob_query_string_2: str
    meter_sas_string: str
    gcp_query_filepath: str
    customer_name: str
    connection_string: str
    containername : str 
    accountname : str 
    querystring : str 

    class Config:
        env_file = "../.env"

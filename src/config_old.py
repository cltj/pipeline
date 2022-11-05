# import os
# from dotenv import load_dotenv


# class My_Config():
#     load_dotenv()
#     def google_application_credentials():
#         GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
#         return GOOGLE_APPLICATION_CREDENTIALS
#     def gcp_data_path():
#         GCP_DATA_PATH = os.getenv("GCP_DATA_PATH")
#         return GCP_DATA_PATH
#     def gcp_dataset_location():
#         DATASET_LOCATION = os.getenv("DATASET_LOCATION")
#         return DATASET_LOCATION
#     def gcp_project_id():
#         GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
#         return GCP_PROJECT_ID
#     def gcp_credential_path():
#         GCP_CREDENTIAL_PATH = os.getenv("GCP_CREDENTIAL_PATH")
#         return GCP_CREDENTIAL_PATH
#     def local_files_path():
#         LOCAL_FILES_PATH = os.getenv("LOCAL_FILES_PATH")
#         return LOCAL_FILES_PATH
#     def svv_billing_account_id():
#         SVV_BILLING_ACCOUNT_ID = os.getenv("SVV_BILLING_ACCOUNT_ID")
#         return SVV_BILLING_ACCOUNT_ID
#     def gcp_gbq_table():
#         GCP_GBQ_TABLE = os.getenv("GCP_GBQ_TABLE")
#         return GCP_GBQ_TABLE
#     def az_subscription_id():
#         AZ_SUBSCRIPTION_ID = os.getenv("AZ_SUBSCRIPTION_ID")
#         return AZ_SUBSCRIPTION_ID
#     def az_tenant_id():
#         AZ_TENANT_ID = os.getenv("AZ_TENANT_ID")
#         return AZ_TENANT_ID
#     def az_client_id():
#         AZ_CLIENT_ID = os.getenv("AZ_CLIENT_ID")
#         return AZ_CLIENT_ID
#     def az_client_secret():
#         AZ_CLIENT_SECRET = os.getenv("AZ_CLIENT_SECRET")
#         return AZ_CLIENT_SECRET
#     def aws_secret_key():
#         AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
#         return AWS_SECRET_KEY
#     def aws_access_id():
#         AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
#         return AWS_ACCESS_ID
#     def storage_container_name_1():
#         STORAGE_CONTAINER_NAME_1 = os.getenv("STORAGE_CONTAINER_NAME_1")
#         return STORAGE_CONTAINER_NAME_1
#     def storage_container_name_2():
#         STORAGE_CONTAINER_NAME_2 = os.getenv("STORAGE_CONTAINER_NAME_2")
#         return STORAGE_CONTAINER_NAME_2
#     def storage_container_name_3():
#         STORAGE_CONTAINER_NAME_3 = os.getenv("STORAGE_CONTAINER_NAME_3")
#         return STORAGE_CONTAINER_NAME_3
#     def storage_connection_string():
#         STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
#         return STORAGE_CONNECTION_STRING
#     def storage_blob_name():
#         STORAGE_BLOB_NAME = os.getenv("STORAGE_BLOB_NAME")
#         return STORAGE_BLOB_NAME
#     def storage_account_name():
#         STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
#         return STORAGE_ACCOUNT_NAME
#     def storage_account_key():
#         STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
#         return STORAGE_ACCOUNT_KEY
#     def blob_query_string():
#         BLOB_QUERY_STRING = os.getenv("BLOB_QUERY_STRING")
#         return BLOB_QUERY_STRING
#     def blob_query_string_2():
#         BLOB_QUERY_STRING_2 = os.getenv("BLOB_QUERY_STRING_2")
#         return BLOB_QUERY_STRING_2
#     def new_container_name():
#         NEW_CONTAINER_NAME = os.getenv("NEW_CONTAINER_NAME")
#         return NEW_CONTAINER_NAME
#     def meter_sas_string():
#         METER_SAS_STRING = os.getenv("METER_SAS_STRING")
#         return METER_SAS_STRING
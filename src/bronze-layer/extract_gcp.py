def gcp_billing():
    from google.cloud import bigquery
    import os, pandas_gbq, time
    import pandas as pd
    from get_gcp_query import gcp_query_files

    
    LOCAL_FILES_PATH = os.environ.get("LOCAL_FILES_PATH")
    GCP_CREDENTIAL_PATH = os.environ.get("GCP_CREDENTIAL_PATH")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_CREDENTIAL_PATH
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    GCP_DATASET_LOCATION = os.environ.get("GCP_DATASET_LOCATION")
    
    client = bigquery.Client(project=GCP_PROJECT_ID, location=GCP_DATASET_LOCATION)

    file_path = os.environ.get("GCP_QUERY_FILEPATH")
    dir_list = os.listdir(file_path)
    for file in dir_list:
        query = gcp_query_files(file_path + "/" + file)
    
        data = pandas_gbq.read_gbq(query, project_id=GCP_PROJECT_ID, dialect='standard')
        df = pd.DataFrame(data)
        
        file = file.replace('.sql','.parquet')
        isExist = os.path.exists(LOCAL_FILES_PATH + 'data/gcp_data/')
        if isExist is False:
            os.mkdir(LOCAL_FILES_PATH + 'data/gcp_data/')
            time.sleep(1)
        
        df.to_parquet(LOCAL_FILES_PATH + "data/gcp_data/" + file, index=True)
    
    print("GCP billing data extracted!")
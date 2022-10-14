def gcp_billing():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import sys, os, pandas_gbq, time
    import pandas as pd
    sys.path.insert(0,'/mnt/c/dev/cl/pipeline/')
    from src.config import My_Config as cfg
    from get_gcp_query import gcp_query_files
    from dotenv import load_dotenv
    load_dotenv()

    
    LOCAL_FILES_PATH = cfg.local_files_path()
    credential_path = cfg.gcp_credential_path()
    gcp_gbq_table = cfg.gcp_gbq_table()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    project_id=cfg.gcp_project_id()
    dataset_location = cfg.gcp_dataset_location()
    
    client = bigquery.Client(project=project_id, location=dataset_location)

    
    file_path = os.environ.get("GCP_QUERY_FILEPATH")
    dir_list = os.listdir(file_path)
    for file in dir_list:
        query = gcp_query_files(file_path + "/" + file)
    
        data = pandas_gbq.read_gbq(query, project_id=project_id, dialect='standard')
        df = pd.DataFrame(data)
        file = file.replace('.sql','.parquet')
        isExist = os.path.exists(LOCAL_FILES_PATH + 'data/gcp_data/')
        if isExist is False:
            os.mkdir(LOCAL_FILES_PATH + 'data/gcp_data/')
            time.sleep(1)
        df.to_parquet(LOCAL_FILES_PATH + "data/gcp_data/" + file, index=True)
    print("GCP billing data extracted!")
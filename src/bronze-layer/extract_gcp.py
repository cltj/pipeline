def gcp_billing():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import sys, os, pandas_gbq
    import pandas as pd
    sys.path.insert(0,'/mnt/c/dev/cl/pipeline/')
    from src.config import My_Config as cfg
    
    LOCAL_FILES_PATH = cfg.local_files_path()
    credential_path = cfg.gcp_credential_path()
    gcp_gbq_table = cfg.gcp_gbq_table()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    project_id=cfg.gcp_project_id()
    dataset_location = cfg.gcp_dataset_location()
    # credentials = service_account.Credentials.from_service_account_file(credential_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(project=project_id, location=dataset_location)
    query = """SELECT * FROM {table}""".format(table="`"+ gcp_gbq_table+"`")
    
    data = pandas_gbq.read_gbq(query, project_id=project_id, dialect='standard')
    df = pd.DataFrame(data)
    df.to_parquet(LOCAL_FILES_PATH + 'GCP-Billing-Data.parquet', index=False)
    print("GCP billing data extracted!")
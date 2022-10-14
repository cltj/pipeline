from multiprocessing.connection import wait


def azure_billing():
    from azure.identity import ClientSecretCredential
    from azure.mgmt.consumption import ConsumptionManagementClient
    import pandas as pd
    import sys, os, time
    sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
    from src.config import My_Config as cfg
    
    # This needs an app registration in azure.
    credential = ClientSecretCredential(
        tenant_id=cfg.az_tenant_id(), 
        client_id=cfg.az_client_id(), 
        client_secret=cfg.az_client_secret()
        )
    LOCAL_FILES_PATH = cfg.local_files_path()
    client = ConsumptionManagementClient(credential=credential,subscription_id=cfg.az_subscription_id())
    scope='/subscriptions/' + str(cfg.az_subscription_id()) + '/'
    usage = client.usage_details.list(scope=scope)
    df = pd.DataFrame()
    for item in usage:
        df = df.append(item.as_dict(), ignore_index=True)
    isExist = os.path.exists(LOCAL_FILES_PATH + 'data/azure_data/')
    if isExist is False:
        os.mkdir(LOCAL_FILES_PATH + 'data/azure_data/')
        time.sleep(1)
        
    df.to_parquet(LOCAL_FILES_PATH + 'data/azure_data/Azure-Billing-Data.parquet')
    print("Azure billing data extracted!")
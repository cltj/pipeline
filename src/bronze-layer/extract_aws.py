def aws_billing():
    import sys, boto3, os
    sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
    from src.config import My_Config as cfg
    
    session = boto3.Session(aws_access_key_id=cfg.aws_access_id(), aws_secret_access_key=cfg.aws_secret_key())
    LOCAL_FILES_PATH = cfg.local_files_path()
    s3 = session.resource('s3')
    bucket = s3.Bucket('collectbill')
    count = 1
    filenames = []
    for s3_object in bucket.objects.all():
        if s3_object.key.endswith('.parquet'):
            path, filename = os.path.split(s3_object.key)
            bucket.download_file(s3_object.key, LOCAL_FILES_PATH + '/AWS-Billing-Data.parquet')
            filenames.append(s3_object.key)
            print("AWS billing data extracted!")
            break
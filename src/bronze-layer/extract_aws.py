def aws_billing():
    import boto3, os, time
    import pandas as pd
    
    LOCAL_FILES_PATH = os.environ.get("LOCAL_FILES_PATH")
    AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
    
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_SECRET_KEY)
    s3 = session.resource('s3')
    bucket = s3.Bucket('collectbill')
    filenames = []
    
    isExist = os.path.exists(LOCAL_FILES_PATH + 'data/aws_data/')
    if isExist is False:
        os.mkdir(LOCAL_FILES_PATH + 'data/aws_data/')
        time.sleep(1)
    
    df = pd.DataFrame()
    for s3_object in bucket.objects.all():
        if s3_object.key.endswith('.parquet'):
            path, filename = os.path.split(s3_object.key)
            bucket.download_file(s3_object.key, LOCAL_FILES_PATH + 'data/aws_data/'+filename)
            part = pd.read_parquet(LOCAL_FILES_PATH + 'data/aws_data/'+filename)
            df = pd.concat([df,part], ignore_index=True)
            os.remove(LOCAL_FILES_PATH + 'data/aws_data/'+filename)
            filenames.append(s3_object.key)
   
    df.to_parquet(LOCAL_FILES_PATH + 'data/aws_data/aws-billing-data.parquet')
    print("AWS billing data extracted!")

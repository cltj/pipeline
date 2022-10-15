
import os, shutil
# sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
# from src.config import My_Config as cfg
from extract_azure import azure_billing
from extract_aws import aws_billing
from extract_gcp import gcp_billing
from upload_to_bronze import upload_parquet


# Env-vars
LOCAL_FILES_PATH = os.environ.get("LOCAL_FILES_PATH")

def cleanup_files():
    """
    Cleans up local parquet files
    Params: None
    Returns: None
    """
    dir_path = LOCAL_FILES_PATH + '/data'
    sub_dir = os.listdir(dir_path)
    for dir in sub_dir:
        try:
            shutil.rmtree(dir_path+"/"+ dir)
        except OSError as e:
            print("Error: %s : %s" % (dir_path, e.strerror))


def main():
    azure_billing()
    aws_billing()
    gcp_billing()
    print("Billing data extracted!")
    upload_parquet()
    print("Billing data uploaded")
    cleanup_files()
    print("Cleaned local data!")


if __name__ == '__main__':
    main()
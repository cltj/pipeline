from src.extract_billing_multicloud import aws_billing, gcp_billing, azure_billing
from src.upload_to_bronze import upload_parquet, cleanup_files


def main():
    aws_billing()
    gcp_billing()
    azure_billing()
    upload_parquet()
    cleanup_files()

    
if __name__ == '__main__':
    main() 
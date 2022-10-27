from split_azure_data import tags_dim, sub_dim, product_dim, resource_dim, usage_fact
from upload_azu_tables_silver import upload_csv

def main():
    # create_singles_csv() # this needs reviewing
    tags_dim()
    sub_dim()
    product_dim()
    resource_dim()
    usage_fact()
    upload_csv()
    print("Done!!!")


if __name__ == '__main__':
    main()

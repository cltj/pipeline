import sys
sys.path.insert(0,'/mnt/c/dev/cl/pipeline')
from src.config import My_Config as cfg
from download_blob import get_data
from clean_data import removed_columns


def short_columns(word): # this only wors for azure files
    if word == 'meter':
        original_meter_cols = ['meter_id','meter_name','meter_category','meter_sub_category','meter_region', 'unit_of_measure','resource_location']
        return original_meter_cols
    elif word == 'resource':
        original_resource_cols = ['subscription_guid','resource_group','resource_location','consumed_service','service_family']
        return original_resource_cols
    elif word == 'billing':
        original_billing_cols = ['subscription_guid','subscription_name','billing_account_id','billing_account_name','billing_profile_id','billing_profile_name']
        return original_billing_cols
    elif word == 'consumption':
        original_consumption_cols = ['meter_id','date','product','quantity','effective_price','unit_price','cost_in_billing_currency','cost_in_pricing_currency','consumed_service','invoice_section_id','invoice_section_name','cost_center','is_azure_credit_eligible','publisher_type','charge_type','frequency','pay_g_price','pricing_model','service_info1','reservation_id','reservation_name','product_order_id','product_order_name']
        return original_consumption_cols
    else:
        print("Error: Did not recognize parameter name!")
        print("Quiting program")
        quit


def split_data(filename, table_kind):
    LOCAL_FILE_PATH = cfg.local_files_path()
    df = get_data()
    removed_cols = removed_columns()
    original_columns = short_columns(filename)
    for col in original_columns:
        if col in removed_cols:
            original_columns.remove(col)
            print("Removed '" + str(col) + "' from original columns")
    df = df.loc[:,original_columns]
    df.to_csv(LOCAL_FILE_PATH + 'split/azure/' + filename + '_details_' + table_kind + '.csv')
    print('Created shortend ' + table_kind + ' table')
    
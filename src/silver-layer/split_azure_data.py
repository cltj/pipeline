import pandas as pd
import json
import os
from download_blob import get_data

LOCAL_FILES_PATH = os.environ.get("LOCAL_FILES_PATH")

def remove_cols():
    data = get_data("cloudlink/azure")
    df = pd.DataFrame(data)
    print(df.columns)


def sub_dim():
    df = get_data('cloudlink/azure')
    subscription_colums = ["SubscriptionId","subscriptionName", "billingAccountId", "billingAccountName", "billingProfileId", "billingProfileName"]
    subscription_dim = pd.DataFrame(df,columns=subscription_colums, copy=True)
    # subscription_dim.index = subscription_dim["SubscriptionId"]
    subscription_dim.drop_duplicates("SubscriptionId", inplace=True)
    subscription_dim.columns
    subscription_dim.to_csv(LOCAL_FILES_PATH + 'split/azure/sub_dim.json')


def product_dim():
    df = get_data('cloudlink/azure')
    product_colums = ["ProductId","ProductName", "resourceLocation", "meterSubCategory", "meterCategory"]
    product_dim = pd.DataFrame(df,columns=product_colums, copy=True)
    # product_dim.index = product_dim["ProductId"]
    product_dim.drop_duplicates("ProductId", inplace=True)
    product_dim.columns
    product_dim.to_csv(LOCAL_FILES_PATH + 'split/azure/product_dim.json')


def resource_dim():
    df = get_data('cloudlink/azure')
    resource_dim = pd.DataFrame(df,columns=["ResourceId"], copy=True)
    resource_dim = resource_dim['ResourceId'].str.split("/", expand=True)
    resource_dim = resource_dim.loc[:, [2,4,6,8]]
    resource_dim.columns = ['SubscriptionId', 'resourceGroupName', 'consumedService', 'resourceName']
    # resource_dim.index = resource_dim["resourceName"]
    resource_dim.drop_duplicates("resourceName", inplace=True)
    resource_dim.columns
    resource_dim.to_csv(LOCAL_FILES_PATH + 'split/azure/resource_dim.json')


# def find_longest():
#     longest = 0
#     for elem in tags_dim['tags']:
#         if elem is None:
#             continue      
#         next = len(elem)
#         if  next <= longest:
#             continue      
#         else:
#             longest = next
#             longest_element = elem
#     return longest_element  
# longest_tag = find_longest()
# print(len(longest_tag), longest_tag)



def tags_dim():
    df = get_data('cloudlink/azure')
    tags_colums = ['ResourceId', 'tags']
    tags_dim = pd.DataFrame(df,columns=tags_colums, copy=True)
    # tags_dim.index = tags_dim['ResourceId']
    tags_dim.columns
    tags_dim.to_csv(LOCAL_FILES_PATH + 'split/azure/tags_dim.json')


def usage_fact():
    df = get_data('cloudlink/azure')
    usage_colums = ["quantity", "effectivePrice", "paygCostInBillingCurrency", "date", "ProductId", "SubscriptionId"]
    usage_fact = pd.DataFrame(df,columns=usage_colums, copy=True)
    # usage_fact.index = usage_fact["ProductId"]
    usage_fact = usage_fact[usage_fact['effectivePrice'] != 0]
    usage_fact.columns
    usage_fact.to_csv(LOCAL_FILES_PATH + 'split/azure/usage_fact.json')


# def main():
#     tags_dim()
#     sub_dim()
#     product_dim()
#     resource_dim()
#     usage_fact()


# if __name__ == "__main__":
#     main()

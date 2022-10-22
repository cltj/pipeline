import pandas as pd
import json
import os
from download_blob import get_data

LOCAL_FILES_PATH = os.environ.get("LOCAL_FILES_PATH")

def remove_cols():
    data = get_data("cloudlink/azure")
    df = pd.DataFrame(data)
    print(df.columns)


def sub_dim(df):
    subscription_colums = ["SubscriptionId","subscriptionName", "billingAccountId", "billingAccountName", "billingProfileId", "billingProfileName"]

    subscription_dim = pd.DataFrame(df,columns=subscription_colums, copy=True)
    subscription_dim.index = subscription_dim["SubscriptionId"]
    subscription_dim.drop_duplicates("SubscriptionId", inplace=True)
    subscription_dim.columns
    return subscription_dim


def product_dim(df):
    product_colums = ["ProductId","ProductName", "resourceLocation", "meterSubCategory", "meterCategory"]

    product_dim = pd.DataFrame(df,columns=product_colums, copy=True)
    product_dim.index = product_dim["ProductId"]
    product_dim.drop_duplicates("ProductId", inplace=True)
    product_dim.columns
    return product_dim


def resource_dim(df):
    resource_dim = pd.DataFrame(df,columns=["ResourceId"], copy=True)
    resource_dim = resource_dim['ResourceId'].str.split("/", expand=True)
    resource_dim = resource_dim.loc[:, [2,4,6,8]]
    resource_dim.columns = ['SubscriptionId', 'resourceGroupName', 'consumedService', 'resourceName']
    resource_dim.index = resource_dim["resourceName"]
    resource_dim.drop_duplicates("resourceName", inplace=True)
    resource_dim.columns
    return resource_dim


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



def tags_dim(df):
    tags_colums = ['ResourceId', 'tags']
    tags_dim = pd.DataFrame(df,columns=tags_colums, copy=True)
    # tags_dim.index = tags_dim['ResourceId']
    tags_dim.columns
    return tags_dim


def usage_fact(df):
    usage_colums = ["quantity", "effectivePrice", "paygCostInBillingCurrency", "date", "ProductId", "SubscriptionId"]

    usage_fact = pd.DataFrame(df,columns=usage_colums, copy=True)
    usage_fact.index = usage_fact["ProductId"]
    usage_fact = usage_fact[usage_fact['effectivePrice'] != 0]
    usage_fact.columns
    return usage_fact


df = get_data('cloudlink/azure')
tags = tags_dim(df)
tags_list = tags.iloc[:,1]

t = pd.DataFrame(tags_list)
t.shape
t.to_json(LOCAL_FILES_PATH + 'data/azure_tags.json')
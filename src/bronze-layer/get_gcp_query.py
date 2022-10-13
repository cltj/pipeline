def get_gcp_query(filepath, days):
    import os
    with open(filepath) as q:
            content = q.read().replace('\n',' ')
            content = content.replace("{table}",os.environ.get("GCP_GBQ_TABLE"))
            content = content.replace("'{days}'", days)
            return content


def gcp_query_files(file):
    import os.path
    if not os.path.isfile(file):
        print('File does not exist.')
    else:
        print(file)
        query = get_gcp_query(file, "5")
        return query
        
# gcp_query_files()
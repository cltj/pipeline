import pandas as pd
from download_blob import get_data


def remove_cols():
    data = get_data("cloudlink/azure")
    df = pd.DataFrame(data)
    print(df.columns)

remove_cols()
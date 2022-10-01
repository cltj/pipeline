import numpy as np
from .download_blob import get_data


def clean_blank_cols():
    df = get_data()
    # Replace all blank values with NaN
    df = df.replace(r'\s+',np.nan,regex=True).replace('',np.nan)
    before = len(df.columns)

    # Drop all columns where all values is NaN
    df = df.dropna(axis=1,how='all')
    after = len(df.columns)
    removed = before-after

    print("Removed "+ str(removed) + " blank columns!")
    return df


def removed_columns():
    removed_columns = []
    df = get_data()
    df2 = clean_blank_cols()
    raw = list(df.columns)
    cleaned = list(df2.columns)
    for column in raw:
        if column in cleaned:
            continue
        else:
            removed_columns.append(column)
    return removed_columns

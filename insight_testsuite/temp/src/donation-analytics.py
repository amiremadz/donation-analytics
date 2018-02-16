import os
import sys
import numpy as np
import pandas as pd
import datetime as dtm
import math

cols = [0, 7, 10, 13, 14, 15]
col_names = ['CMTE_ID', 'NAME', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']
out_cols  = ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_YR', 'QUANTILE', 'SUM_SOFAR', 'COUNT_SOFAR']

def validate_date(df):
    length = df.shape[0]
    valid = pd.Series(np.ones(length, dtype=bool), index=range(1, length + 1))
    for idx, date in enumerate(df["TRANSACTION_DT"]):
        try:
            dtm.datetime.strptime(date, "%m%d%Y")
        except ValueError:
            valid.iloc[idx] = False
    return df[valid]

def validate_zipcode(df):
    length = df.shape[0]
    valid = df["ZIP_CODE"].map(lambda s: len(s) >= 5)
    return df[valid]

def validate_name(df):
    length = df.shape[0]
    valid = pd.Series(np.ones(length, dtype=bool), index=range(1, length + 1))
    for idx, name in enumerate(df["NAME"]):
        if len(name.split(',')) != 2:
            valid.iloc[idx] = False	
    return df[valid]

def find_precentile(percentile_fullpath):
    with open(percentile_fullpath) as fh:
        percentile = fh.read()
    percentile = float(percentile.strip()) 
    return percentile

def preprocess(input_fullpath):	
    df = pd.read_csv(input_fullpath, header=None, delimiter='|')
    df = df[cols]
    df.columns = col_names

    # Because we are only interested in individual contributions, 
    # we only want records that have the field, OTHER_ID, set to empty 
    df = df[df["OTHER_ID"].isnull()]

    # If TRANSACTION_DT is an invalid date (e.g., empty, malformed)
    df = df[~ df["TRANSACTION_DT"].isnull()]
    df["TRANSACTION_DT"] = df["TRANSACTION_DT"].map(lambda td: str(td))
    df = validate_date(df)

    # Output: 4-digit year of the contribution
    df["TRANSACTION_YR"] = df["TRANSACTION_DT"].map(lambda td: td[-4:])

    # If ZIP_CODE is an invalid zip code (i.e., empty, fewer than five digits)
    df = df[~ df["ZIP_CODE"].isnull()]
    df["ZIP_CODE"] = df["ZIP_CODE"].map(lambda z: str(z).zfill(9))
    df = validate_zipcode(df)

    # While the data dictionary has the ZIP_CODE occupying nine characters, 
    # for the purposes of the challenge, we only consider the first five characters 
    # of the field as the zip code
    df["ZIP_CODE"] = df["ZIP_CODE"].map(lambda z: z[0:5])

    # If the NAME is an invalid name (e.g., empty, malformed)
    df = df[~ df["NAME"].isnull()]
    df = validate_name(df)

    # If any lines in the input file contains empty cells in the CMTE_ID 
    # or TRANSACTION_AMT fields
    df = df[~ df["CMTE_ID"].isnull()]
    df = df[~ df["TRANSACTION_AMT"].isnull()]
    df = df[df["TRANSACTION_AMT"] > 0]

    # Drop redundant columns
    df = df.drop(['TRANSACTION_DT', 'OTHER_ID'], axis=1)
    return df

def postprocess(df):
    df = df[df.duplicated(subset = {'NAME', 'ZIP_CODE'}, keep='first')]
    df = df.reset_index(drop=True)
		
    df['SUM_SOFAR']   = df.groupby(['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_YR'])['TRANSACTION_AMT'].cumsum()
    df['COUNT_SOFAR'] = df.groupby(['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_YR'])['TRANSACTION_AMT'].cumcount().apply(lambda x: x + 1)
    df['QUANTILE'] = np.nan

    for idx, _ in enumerate(df['TRANSACTION_AMT']):
        idx_quantile = int(math.ceil((idx + 1) * (percentile / 100.0)) - 1)
        df['QUANTILE'][idx] = round(df['TRANSACTION_AMT'][:idx+1].sort_values(inplace=False)[idx_quantile])

    df['QUANTILE'] = df['QUANTILE'].astype('int')

    # Drop redundant columns and re-arrange to correct format
    df = df.drop(['NAME', 'TRANSACTION_AMT'], axis=1)
    df = df[out_cols]
    return df

if __name__ == "__main__":
    input_fullpath = sys.argv[1]
    percentile_fullpath = sys.argv[2]
    output_fullpath = sys.argv[3]

    percentile = find_precentile(percentile_fullpath)
    df = preprocess(input_fullpath)
    df = postprocess(df)
    print(df)
    df.to_csv(output_fullpath, sep='|', header=False, index=False)

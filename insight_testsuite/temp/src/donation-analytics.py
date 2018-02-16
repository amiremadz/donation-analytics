import os
import sys
import numpy as np
import pandas as pd
import datetime as dtm
import math

cols = [0, 7, 10, 13, 14, 15]
col_names = ['CMTE_ID', 'NAME', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']
out_cols  = ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_YR', 'QUANTILE', 'SUM_SOFAR', 'COUNT_SOFAR']


class RepeatDoner(object):
    def __init__(self, input_fullpath, percentile_fullpath, output_fullpath):
        self.input_fullpath = input_fullpath
        self.percentile_fullpath = percentile_fullpath
        self.output_fullpath = output_fullpath
        self.df = pd.DataFrame()
        self.percentile = 0

    def run(self):
        self.__find_precentile()
        self.__preprocess()
        self.__postprocess()
        self.__save_result()
        print(self.df)

    def __validate_date(self):
        length = self.df.shape[0]
        valid = pd.Series(np.ones(length, dtype=bool), index=range(1, length + 1))
        for idx, date in enumerate(self.df["TRANSACTION_DT"]):
            try:
                dtm.datetime.strptime(date, "%m%d%Y")
            except ValueError:
                valid.iloc[idx] = False
        self.df = self.df[valid]

    def __validate_zipcode(self):
        length = self.df.shape[0]
        valid = self.df["ZIP_CODE"].map(lambda s: len(s) >= 5)
        self.df = self.df[valid]

    def __validate_name(self):
        length = self.df.shape[0]
        valid = pd.Series(np.ones(length, dtype=bool), index=range(1, length + 1))
        for idx, name in enumerate(self.df["NAME"]):
            if len(name.split(',')) != 2:
                valid.iloc[idx] = False	
        self.df = self.df[valid]

    def __find_precentile(self):
        with open(self.percentile_fullpath) as fh:
            percentile = fh.read()
        self.percentile = float(percentile.strip()) 

    def __preprocess(self):	
        self.df = pd.read_csv(self.input_fullpath, header=None, delimiter='|')
        self.df = self.df[cols]
        self.df.columns = col_names

        # Because we are only interested in individual contributions, 
        # we only want records that have the field, OTHER_ID, set to empty 
        self.df = self.df[self.df["OTHER_ID"].isnull()]

        # If TRANSACTION_DT is an invalid date (e.g., empty, malformed)
        self.df = self.df[~ self.df["TRANSACTION_DT"].isnull()]
        self.df["TRANSACTION_DT"] = self.df["TRANSACTION_DT"].map(lambda td: str(td))
        self.__validate_date()

        # Output: 4-digit year of the contribution
        self.df["TRANSACTION_YR"] = self.df["TRANSACTION_DT"].map(lambda td: td[-4:])

        # If ZIP_CODE is an invalid zip code (i.e., empty, fewer than five digits)
        self.df = self.df[~ self.df["ZIP_CODE"].isnull()]
        self.df["ZIP_CODE"] = self.df["ZIP_CODE"].map(lambda z: str(z).zfill(9))
        self.__validate_zipcode()

        # While the data dictionary has the ZIP_CODE occupying nine characters, 
        # for the purposes of the challenge, we only consider the first five characters 
        # of the field as the zip code
        self.df["ZIP_CODE"] = self.df["ZIP_CODE"].map(lambda z: z[0:5])

        # If the NAME is an invalid name (e.g., empty, malformed)
        self.df = self.df[~ self.df["NAME"].isnull()]
        self.__validate_name()

        # If any lines in the input file contains empty cells in the CMTE_ID 
        # or TRANSACTION_AMT fields
        self.df = self.df[~ self.df["CMTE_ID"].isnull()]
        self.df = self.df[~ self.df["TRANSACTION_AMT"].isnull()]
        self.df = self.df[self.df["TRANSACTION_AMT"] > 0]

        # Drop redundant columns
        self.df = self.df.drop(['TRANSACTION_DT', 'OTHER_ID'], axis=1)

    def __postprocess(self):
        self.df = self.df[self.df.duplicated(subset = {'NAME', 'ZIP_CODE'}, keep='first')]
        self.df = self.df.reset_index(drop=True)
                    
        self.df['SUM_SOFAR']   = self.df.groupby(['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_YR'])['TRANSACTION_AMT'].cumsum()
        self.df['COUNT_SOFAR'] = self.df.groupby(['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_YR'])['TRANSACTION_AMT'].cumcount().apply(lambda x: x + 1)
        self.df['QUANTILE'] = np.nan

        for idx, _ in enumerate(self.df['TRANSACTION_AMT']):
            idx_quantile = int(math.ceil((idx + 1) * (self.percentile / 100.0)) - 1)
            self.df['QUANTILE'][idx] = round(self.df['TRANSACTION_AMT'][:idx+1].sort_values(inplace=False)[idx_quantile])

        self.df['QUANTILE'] = self.df['QUANTILE'].astype('int')

        # Drop redundant columns and re-arrange to correct format
        self.df = self.df.drop(['NAME', 'TRANSACTION_AMT'], axis=1)
        self.df = self.df[out_cols]

    def __save_result(self):
        self.df.to_csv(self.output_fullpath, sep='|', header=False, index=False)

if __name__ == "__main__":
    input_fullpath = sys.argv[1]
    percentile_fullpath = sys.argv[2]
    output_fullpath = sys.argv[3]

    rep_doners = RepeatDoner(input_fullpath, percentile_fullpath, output_fullpath)
    rep_doners.run()

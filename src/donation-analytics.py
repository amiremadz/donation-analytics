import os
import csv
import numpy as np
import pandas as pd
import datetime as dtm

data_file = "itcont.txt"
pcnt_file = "percentile.txt"

output_file = "repeat_donors_amir.txt"

output_path = "../insight_testsuite/tests/test_1/output"
input_path = "../insight_testsuite/tests/test_1/input/"

cols = [0, 7, 10, 13, 14, 15]
col_names = ['CMTE_ID', 'NAME', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']

with open(os.path.join(input_path, pcnt_file)) as fh:
	percentile = fh.read()

percentile = float(percentile.strip()) 

df = pd.read_csv(os.path.join(input_path, data_file), header=None, delimiter='|')
df = df[cols]
df.columns = col_names

# Because we are only interested in individual contributions, 
# we only want records that have the field, OTHER_ID, set to empty 
df = df[df["OTHER_ID"].isnull()]

def validate_date(df):
	length = df.shape[0]
	valid = pd.Series(np.ones(length, dtype=bool), index=range(1, length + 1))
	for idx, date in enumerate(df["TRANSACTION_DT"]):
		try:
			dtm.datetime.strptime(date, "%m%d%Y")
		except ValueError:
			valid.iloc[idx] = False
	return df[valid]

# If TRANSACTION_DT is an invalid date (e.g., empty, malformed)
df = df[~ df["TRANSACTION_DT"].isnull()]
df["TRANSACTION_DT"] = df["TRANSACTION_DT"].map(lambda td: str(td))
df = validate_date(df)

# Output: 4-digit year of the contribution
df["TRANSACTION_YR"] = df["TRANSACTION_DT"].map(lambda td: td[-4:])

def validate_zipcode(df):
	length = df.shape[0]
	valid = df["ZIP_CODE"].map(lambda s: len(s) >= 5)
	return df[valid]

# If ZIP_CODE is an invalid zip code (i.e., empty, fewer than five digits)
df = df[~ df["ZIP_CODE"].isnull()]
df["ZIP_CODE"] = df["ZIP_CODE"].map(lambda z: str(z))
df = validate_zipcode(df)

# While the data dictionary has the ZIP_CODE occupying nine characters, 
# for the purposes of the challenge, we only consider the first five characters 
# of the field as the zip code
df["ZIP_CODE"] = df["ZIP_CODE"].map(lambda z: z[0:5])

def validate_name(df):
	length = df.shape[0]
	valid = pd.Series(np.ones(length, dtype=bool), index=range(1, length + 1))
	for idx, name in enumerate(df["NAME"]):
		if len(name.split(',')) != 2:
			valid.iloc[idx] = False	
	return df[valid]

# If the NAME is an invalid name (e.g., empty, malformed)
df = df[~ df["NAME"].isnull()]
df = validate_name(df)

# If any lines in the input file contains empty cells in the CMTE_ID 
# or TRANSACTION_AMT fields
df = df[~ df["CMTE_ID"].isnull()]
df = df[~ df["TRANSACTION_AMT"].isnull()]

print(df)
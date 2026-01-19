import pandas as pd
import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.core.batch import Batch
from typing import List,Union
from lib.validations import *

## load the dataset first:
df=pd.read_csv("data/extracted_df.csv")

## create the batch:
batch=create_batch(df=df)

## create the min expectations:
volume_min_exp=create_min_expectations(min_value=0,max_value=0,column_name="Volume")
volume_max_exp=create_max_expectations(min_value=0,max_value=0,colum_name="Volume")

## create the open min expectations:
open_min_exp=create_min_expectations(min_value=1,max_value=1.3,column_name="Open")
open_max_exp=create_max_expectations(min_value=1,max_value=1.3,column_name="Open")

## create the close expectations:
close_min_exp=create_min_expectations(min_value=1,max_value=1.3,column_name="Close")
close_max_exp=create_max_expectations(min_value=1,max_value=1.3,column_name="Close")

exp_labels:List[str]=["volume_min","volume_max","open_min","open_max","close_min","close_max"]

expectations:List[ExpectationConfiguration]=[volume_min_exp,volume_max_exp,open_min_exp,open_max_exp,close_min_exp,close_max_exp]

## perform some data validations:
results_df=validate_expectations(batch=batch,expectations=expectations,exp_labels=exp_labels)

## save the df to a csv format:
results_df.to_csv("data_quality_checks/extract_checks_df.csv")

import pandas as pd
import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.core.batch import Batch
from typing import List,Union
from lib.validations import *

## load the dataset first:
df=pd.read_csv("data/transformed_df.csv")

## create the batch:
batch=create_batch(df=df)

## create the min expectations:
volume_min_exp=create_min_expectations(min_value=0,max_value=0,column_name="Volume")

## create the pips expectations:
pip_min_exp=create_min_expectations(min_value=1,max_value=100,column_name="Pips")
pip_max_exp=create_max_expectations(min_value=200,max_value=1000,column_name="Pips")


## create the expectations list:
expectations:List[ExpectationConfiguration]=[volume_min_exp,pip_min_exp,pip_max_exp]

exp_labels:List[str]=["volume_min","pips_min","pips_max"]

transform_df=validate_expectations(batch=batch,expectations=expectations,labels=exp_labels)

## save the transform df:
transform_df.to_csv("data_quality_checks/transform_checks_df.csv",index=False)

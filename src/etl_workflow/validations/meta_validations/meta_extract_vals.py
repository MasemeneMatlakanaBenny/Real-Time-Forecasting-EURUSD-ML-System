import pandas as pd
from prefect import task,flow
from lib.validations import *

## load the extract validations dataframe results that was saved in a csv format:
results_df=pd.read_csv("data_quality_checks/extract_checks_df.csv")

## create the batch first:
batch=create_batch(df=results_df)

## run the meta validations:
checks_results=meta_validation(batch=Batch,expectation_results=results_df)


## print the checks results-if success then data quality checks passed
print(checks_results)

import pandas as pd
from prefect import task,flow
from great_expectations.core.batch import Batch
from lib.validations import *

## load the extract validations dataframe results that was saved in a csv format:
@task
def load_results_df(path:str)->pd.DataFrame:
    """
    """

    results_df=pd.read_csv("data_quality_checks/transform_checks_df.csv")

    return results_df

@task
def data_batch(results_df:pd.DataFrame)->Batch:
    """
    """

    ## create the batch first:
    batch=create_batch(df=results_df)

    return batch

@task
def meta_quality_checks(batch:Batch,results_df:pd.DataFrame)->str:

    ## run the meta validations:
    checks_results=meta_validation(batch=Batch,expectation_results=results_df)

    ## print the checks results-if success then data quality checks passed
    print(checks_results)

@flow
def meta_extract_validation_pipeline():
    """
    """
    results_df=load_results_df()

    ## data batch -will be used to perform data validations:
    batch=data_batch()

    ## perform data quality checks on top of data quality checks:
    meta_results=meta_quality_checks(batch=batch,results_df=results_df)


if __name__=="__main__":
    meta_extract_validation_pipeline()

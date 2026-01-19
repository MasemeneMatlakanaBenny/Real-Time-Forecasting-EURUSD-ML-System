import pandas as pd
import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.core.batch import Batch
from prefect import task,flow
from typing import List,Union
from lib.validations import *
## load the dataset first:
@task
def load_extracted_df(path:str)->pd.DataFrame:
    """
    Docstring for load_extracted_df
    
    :param path: Description
    :type path: str
    :return: Description
    :rtype: DataFrame
    """
    df=pd.read_csv("data/extracted_df.csv")

    return df

## create the batch:
@task
def data_batch(df:pd.DataFrame)->Batch:
    """
    Docstring for data_batch
    
    :param df: Description
    :type df: pd.DataFrame
    :return: Description
    :rtype: Batch
    """
    batch=create_batch(df=df)

    return batch

@task
def data_expectations(batch:Batch):
    """
    Docstring for data_expectations
    
    :param batch: Description
    :type batch: Batch
    """
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
@task
def test_data_quality_checks(batch:Batch,expectations:List[ExpectationConfiguration],exp_labels:List[str]):
    """
    Docstring for test_data_quality_checks
    
    :param batch: Description
    :type batch: Batch
    :param expectations: Description
    :type expectations: List[ExpectationConfiguration]
    :param exp_labels: Description
    :type exp_labels: List[str]
    """
    results_df=validate_expectations(batch=batch,expectations=expectations,exp_labels=exp_labels)
    ## save the df to a csv format:
    results_df.to_csv("data_quality_checks/extract_checks_df.csv")

## run the pipeline with an automation/automated workflow:
@flow
def data_extraction_validation_pipeline(df_path):
    """
    """

    df=load_extracted_df(path=df_path)

    batch=data_batch(df=df)

    expectations,exp_labels=data_expectations()

    test_data_quality_checks(batch=batch,expectations=expectations,exp_labels=exp_labels)



if __name__=="__main__":
    data_extraction_validation_pipeline(df_path="data/extracted_df.csv")

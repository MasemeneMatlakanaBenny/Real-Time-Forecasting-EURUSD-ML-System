import pandas as pd
import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.core.batch import Batch
from typing import List,Union
from prefect import task,flow
from lib.validations import *

## load the dataset first:

@task
def load_transformed_df(path)->pd.DataFrame:
    """
    Docstring for load_transformed_df
    
    :param path: Description
    """
    df=pd.read_csv(path)

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

## create the min expectations:
@task
def data_expectations():
    volume_min_exp=create_min_expectations(min_value=0,max_value=0,column_name="Volume")

    ## create the pips expectations:
    pip_min_exp=create_min_expectations(min_value=1,max_value=100,column_name="Pips")
    pip_max_exp=create_max_expectations(min_value=200,max_value=1000,column_name="Pips")

    ## create the expectations list:
    expectations:List[ExpectationConfiguration]=[volume_min_exp,pip_min_exp,pip_max_exp]
    exp_labels:List[str]=["volume_min","pips_min","pips_max"]

    return expectations,exp_labels

@task
def test_data_quality_checks(batch:Batch,expectations:List[ExpectationConfiguration],exp_labels:List[str])->pd.DataFrame:

    transform_df=validate_expectations(batch=batch,expectations=expectations,labels=exp_labels)

    ## save the transform df:
    transform_df.to_csv("data_quality_checks/transform_checks_df.csv",index=False)

@flow
def transformation_validation_pipeline(df_path):
    """
    Docstring for transformation_validation_pipeline
    
    :param df_path: Description
    """

    df=load_transformed_df(path=df_path)

    batch=data_batch(df=df)

    expectations,exp_labels=data_expectations()

    test_data_quality_checks(batch=batch,expectations=expectations,exp_labels=exp_labels)


if __name__=="__main__":
    transformation_validation_pipeline(df_path="data/transformed_df.csv")

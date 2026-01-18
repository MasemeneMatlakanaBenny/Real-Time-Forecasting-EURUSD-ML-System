import pandas as pd
import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.core.batch import Batch
from typing import List,Union

def create_batch(df:pd.DataFrame)->Batch:
    """
    Docstring for create_batch
    
    :param df: dataframe for validations
    :type df: pd.DataFrame
    :return: batch used for validating the data expectations for the passed dataframe
    :rtype: Batch
    """
    ## create the context first:
    context=gx.get_context()

    ## create the data source:
    data_source=context.data_sources.add_pandas("pandas_df")

    ## create the data asset:
    data_asset=data_source.add_dataframe_asset("data_asset")

    ## create the batch definition:
    batch_definition=data_asset.add_batch_definition_whole_dataframe("batch_definition")

    ## create the batch:
    batch=batch_definition.get_batch(batch_parameters={"df":df})

    return batch

## create the data expectations now:
## create the min expectations:
def create_min_expectations(min_value:Union[int,float],
                            max_value:Union[int,float],
                            column_name)->ExpectationConfiguration:
    """
    Docstring for create_min_expectations
    
    :param min_value: expected minimum value for the column
    :type min_value: Union[int, float]
    :param max_value: expected maximum value for the column
    :type max_value: Union[int, float]
    :param column_name: name of the column to create the expectation for
    :return: expectation that will be validated  or tested
    :rtype: ExpectationConfiguration
    """
    expectation=gx.expectations.ExpectColumnMinToBeBetween(
        column=column_name,min_value=min_value,max_value=max_value

    )

    return expectation

## create the max_expectations:
def create_max_expectations(min_value:Union[int,float],
                            max_value:Union[int,float],
                            column_name)->ExpectationConfiguration:
    """
    Docstring for create_max_expectations
    
    :param min_value: expected minimum value for the column
    :type min_value: Union[int, float]
    :param max_value: expected maximum value for the column
    :type max_value: Union[int, float]
    :param column_name: the name of the column to be validated
    :return: a data expectation to be validated or tested
    :rtype: ExpectationConfiguration
    """

    expectation=gx.expectations.ExpectColumnMaxToBeBetween(
        column=column_name,min_value=min_value,max_value=max_value
    )

    return expectation

## create the function that will validate data expectations:
def validate_expectations(batch:Batch,
                          expectations:List[ExpectationConfiguration],
                          labels:List[str])->pd.DataFrame:
    """
    Docstring for validate_expectations
    
    :param batch: batch created for validating the data expectations
    :type batch: Batch
    :param expectations: list of the data expectations
    :type expectations: List[ExpectationConfiguration]
    :param labels: labels of the data expectations
    :type labels: List[str]
    :return: dataframe for the expectation results after validating them
    :rtype: DataFrame
    """
    exp_results=[]
    for exp in expectations:
        validation=batch.validate(exp)

        results=validation[0]

        exp_results.append(results)
    
    df=pd.DataFrame({
        "expectation":labels,
        "results":exp_results
    })

    return df

def meta_validation(batch:Batch,expectation_results:pd.DataFrame)->str:
    """
    Docstring for meta_validation
    
    :param batch: batch for meta data validation
    :type batch: Batch
    :param expectation_results: validated expectations results
    :type expectation_results: pd.DataFrame
    :return: Description
    :rtype: str
    """

    ## create the expectation:
    expectation:ExpectationConfiguration=gx.expectations.ExpectColumnDistinctValuesToEqualSet(
        column_name="results",value_set=["success"]
    )

    validation=batch.validate(expectation)

    return validation[0]



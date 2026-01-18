import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import Batch
from great_expectations.expectations.expectation import ExpectationConfiguration
from typing import List,Union

## create the batch expectation:
def create_batch_expectations(df:pd.DataFrame)->Batch:
    """
    Docstring for create_batch_expectations
    
    :param df: Description
    :type df: pd.DataFrame
    :return: Description
    :rtype: Batch
    """

    ## create the context:
    context=gx.get_context()

    ## create the data source within the context:
    data_source=context.data_sources.add_pandas("pandas_metrics_df")

    ## create the data asset:
    data_asset=data_source.add_dataframe_asset("metrics_asset")

    ## create the batch definition:
    batch_definition=data_asset.add_batch_definition_whole_dataframe("batch_frame")

    ## create the batch-> will be returned:
    batch=batch_definition.get_batch(batch_parameters={"df":df})

    return batch


## create the expectation:
def create_metrics_expectation(min_value:Union[int,float],
                               max_value:Union[int,float],
                               column_name:str)->ExpectationConfiguration:
    """
    Docstring for create_metrics_expectation
    
    :param min_value: expected minimum value of the column
    :type min_value: Union[int, float]
    :param max_value: expected maximum value of the column
    :type max_value: Union[int, float]
    :param column_name: name of the column to create the expectation for
    :type column_name: str
    :return: Description
    :rtype: ExpectationConfiguration
    """

    expectation:ExpectationConfiguration=gx.expectations.ExpectColumnValuesToBeBetween(
        column=column_name,min_value=min_value,max_value=max_value
    )

    return expectation

def validate_model_expectations(batch:Batch,
                                expectations:List[ExpectationConfiguration],
                                labels:List[str])->pd.DataFrame:
    """
    Docstring for validate_model_expectations
    
    :param batch: Description
    :type batch: Batch
    :param expectations: Description
    :type expectations: List[ExpectationConfiguration]
    :param labels: Description
    :type labels: List[str]
    :return: Description
    :rtype: DataFrame
    """
    
    exp_results=[]
    for exp in expectations:
        validation=batch.validate(exp)

        results=validation[0]

        exp_results.append(exp)

    df=pd.DataFrame({"expectations":expectations,"results":exp_results})

    return df

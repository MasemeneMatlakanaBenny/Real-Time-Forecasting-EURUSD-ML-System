import pandas as pd
import numpy as np
from prefect import task,flow

@task
def get_extracted_df(path:str)->pd.DataFrame:
    """
    Docstring for get_extracted_df
    
    :param path: path for the extracted data
    :type path: str
    :return: return the extracted dataframe
    :rtype: DataFrame
    """
    df=pd.read_csv(path)

    return df
    
@task
def drop_index_column(df:pd.DataFrame)->pd.DataFrame:
    """
    Docstring for drop_index_column
    
    :param df: extracted dataframe
    :type df: pd.DataFrame
    :return: return the dataframe but now with renamed column names
    :rtype: DataFrame
    """
    ## start first with the index column:
    df.columns = df.columns.droplevel(1)

    ## reset the index for the dataframe:
    df=df.reset_index()

    return df

@task
def convert_datetime_column(df:pd.DataFrame)->pd.DataFrame:
    """
    Docstring for convert_datetime_column
    
    :param df: the extracted dataframe
    :type df: pd.DataFrame
    :return: the intial dataframe but now with Datetime using Johannesburg timezone
    :rtype: DataFrame
    """
    ## convert the datetime timezone from UCT to South African timezone:
    df['Datetime'] = df['Datetime'].dt.tz_convert('Africa/Johannesburg')

    return df

@task
def calculate_pair_pips(df:pd.DataFrame)->pd.DataFrame:
    """
    Docstring for calculate_pair_pips
    
    :param df: Description
    :type df: pd.DataFrame
    :return: Description
    :rtype: DataFrame
    """
    df['high_decimal_int'] = (
    df['High']
      .astype(str)
      .str.split('.')
      .str[1]
      .str[:6]                    # keep only first 6 digits
      .str.pad(width=6, side='right', fillchar='0')  # pad if shorter
      .astype(int)
      )
    
    df['low_decimal_int'] = (
    df['Low']
      .astype(str)
      .str.split('.')
      .str[1]
      .str[:6]                    # keep only first 6 digits
      .str.pad(width=6, side='right', fillchar='0')  # pad if shorter
      .astype(int)
      )
    
    df['Pips']=np.abs(df['high_decimal_int']-df['low_decimal_int'])

    return df

@task
def remove_zero_pips_days(df:pd.DataFrame)->pd.DataFrame:
    """
    Docstring for remove_zero_pips_days
    
    :param df: Description
    :type df: pd.DataFrame
    :return: Description
    :rtype: DataFrame
    """
    df=df[df['Pips']>0]

@flow
def transformation_workflow():
    """"
    """

    df=get_extracted_df(path="data/extracted_df.csv")

    df=drop_index_column(df=df)

    df=convert_datetime_column(df=df)

    df=calculate_pair_pips(df=df)

    df=remove_zero_pips_days(df=df)

    df.to_csv("data/transformed_df.csv",index=False)
    

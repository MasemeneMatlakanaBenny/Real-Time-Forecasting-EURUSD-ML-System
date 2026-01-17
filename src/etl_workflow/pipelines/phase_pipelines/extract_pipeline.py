import yfinance as yf
import pandas as pd
from prefect import task,flow


@task
def download_data():
    """
    Docstring for download_data
    """
    # Download data
    df = yf.download(
    tickers="EURUSD=X",
    period="30d",
    interval="1h",
    auto_adjust=False,
    progress=False
    )

    return df


@task
def save_extracted_df(df:pd.DataFrame):
    """
    Docstring for save_extracted_df
    
    :param df: extracted data in a dataframe
    :type df: pd.DataFrame
    """

    df.to_csv("data/extracted_df.csv",index=False)

@flow
def extract_workflow():
    """
    Docstring for extract_workflow
    """
    df=download_data()

    save_extracted_df(df=df)


if __name__=="__main__":
    extract_workflow()

    

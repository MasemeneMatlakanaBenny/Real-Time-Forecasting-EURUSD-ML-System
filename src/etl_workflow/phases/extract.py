import yfinance as yf
import pandas as pd


# Download data
df = yf.download(
    tickers="EURUSD=X",
    period="30d",
    interval="1h",
    auto_adjust=False,
    progress=False
)

## save the df as csv file:
df.to_csv("data/extracted_df.csv",index=False)

import pandas as pd
import numpy as np

df=pd.read_csv("data/extracted_df.csv")

## start first with the index column:
df.columns = df.columns.droplevel(1)

## reset the index for the dataframe:
df=df.reset_index()


## convert the datetime timezone from UCT to South African timezone:
df['Datetime'] = df['Datetime'].dt.tz_convert('Africa/Johannesburg')


## calculate the daily pips:
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

## if there are any observations where the Pips =0 then it means one of the following:
###-Market closed
###-Weekends
###-Holidays

df=df[df['Pips']>0]


## save the dataframe after transform it:
df.to_csv("data/transformed_df.csv",index=False)

import pandas as pd
import joblib
from statsmodels.tsa.arima.model import ARIMA

## load the transformed dataset:
df=pd.read_csv("data/transformed_df.csv")

## save the datetime and high only:
df=df[['Datetime','Low']]

## develop the model:
model=ARIMA(df['Low'],order=(1,0,0))

## fit the model:
model_fitted=model.fit()

## save the model as pickle file:
joblib.dump(model,"models/low_models/low_arima_model.pkl")



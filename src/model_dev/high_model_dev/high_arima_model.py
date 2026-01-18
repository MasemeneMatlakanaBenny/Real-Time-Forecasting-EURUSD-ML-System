import pandas as pd
import joblib
from statsmodels.tsa.arima.model import ARIMA

## load the transformed dataset:
df=pd.read_csv("data/transformed_df.csv")

## save the datetime and high only:
df=df[['Datetime','High']]

## develop the model:
model=ARIMA(df['High'],order=(1,0,0))

## fit the model:
model_fitted=model.fit()

## save the model as pickle file:
joblib.dump(model,"models/high_models/high_arima_model.pkl")


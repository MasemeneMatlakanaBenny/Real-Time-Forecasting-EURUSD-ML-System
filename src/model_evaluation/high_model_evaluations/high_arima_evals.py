import pandas as pd
import numpy as np
import joblib
from lib.configs import compute_model_metrics

## load the model first:
model=joblib.load("models/low_models/high_arima_model.pkl")

## get the forecasts:
df=pd.read_csv("data/transformed_df.csv")

test_len=np.round(0.2*len(df),0)

## forecasts:
forecasts=model.forecast(test_len)


## compute the model metrics:
metrics=compute_model_metrics(y_true=forecasts,y_preds=forecasts)

joblib.dump(metrics,"metrics/high_metrics/high_arima_metrics.pkl")

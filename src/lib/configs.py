import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error,mean_absolute_error,r2_score

## create a function that will split the dataset train/test sets into X_test,y_test or X_train and y_train:
def train_set_test_set(df:pd.DataFrame):
    ## prepare the data for train and test sets:
    df_len=len(df)
    train_len=np.round(0.8*df_len,0)
    test_len=np.round(0.2*df_len,0)

    train_data=df[:int(train_len.item())]
    test_data=df[int(train_len.item()):]

    return train_data,test_data


## create the functiont that returns the model metrics in a dictionary format:
def compute_model_metrics(y_true,y_preds):
    """"
    """

    mse=mean_squared_error(y_true,y_preds)
    mae=mean_absolute_error(y_true,y_preds)
    r2_model=r2_score(y_true,y_preds)

    metrics={
        "MSE":mse,
        "MAE":mae,
        "R-squared":r2_model
    }

    return metrics



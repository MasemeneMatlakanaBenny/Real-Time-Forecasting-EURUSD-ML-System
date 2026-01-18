# EURUSD REAL TIME ML SYSTEM

This is a real time system that predicts the EURUSD next movements in real time.The workflow for the project covers data engineering,data science ,machine learning and data analysis as well as operations to maintain and
optimize the system in the production environment. 
It all starts by extracting the data using the yfinance api ,transforming it and performing the data quality checks to ensure that the data is accurate and clean for machine learning consumption.
Once all that is done,models are developed along with pipelines to ensure retraining in the near future. As such it should be noted that software engineering also plays a huge role in the project as modularity ,reusability
and encapsulation are all applied to ensure code optimization and the need to rewrite everything from the scratch in every file.
Model performance is taken into factor to make sure that the models going into the production phase are much better predictors .
However drift detection is another concept that tends to happens especially when working with such data that might slightly get influenced by events and economic trends and markets.

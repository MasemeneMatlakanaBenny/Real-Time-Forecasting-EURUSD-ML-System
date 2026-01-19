# EURUSD REAL TIME ML SYSTEM

This is a real time system that predicts the EURUSD next movements in real time.The workflow for the project covers data engineering,data science ,machine learning and data analysis as well as operations to maintain and
optimize the system in the production environment. 
It all starts by extracting the data using the yfinance api ,transforming it and performing the data quality checks to ensure that the data is accurate and clean for machine learning consumption.
Once all that is done,models are developed along with pipelines to ensure retraining in the near future. As such it should be noted that software engineering also plays a huge role in the project as modularity ,reusability
and encapsulation are all applied to ensure code optimization and the need to rewrite everything from the scratch in every file.
Model performance is taken into factor to make sure that the models going into the production phase are much better predictors .
However drift detection is another concept that tends to happens especially when working with such data that might slightly get influenced by events and economic trends and markets.

Tools and libraries used for the workflow are found under the requirements.txt file in the repo.

----

## Structure Of The Project:

The structure of the project can be seen in the below image:

![LIB](images/STRUCTURE/Structure.png)

We have the lib folder which is where software engineering is executed. Libraries and Modules are designed specifically meant for the project in order to maintain reusability and modularity. 
Hence we aim to apply software engineering concepts first to optimize code performance and resource optimizations on our local machines. For each of the phases in the workflow ,there is a specific module designed for it in the lib folder hence modularity has been applied as said before. We break down components that are reusable into smaller parts that can be imported over and over again.

The etl_workflow is the folder in which we perform the ETL workflow. That is we extract the data,transform it and load it into the lakehouse. We also perform data quality checks in this phase. This is essential because we need the data first in order to build and develop our predictive machine learning models for the scope. Without data,nothing can be done.Hence this is the first phase of the entire workflow.
The etl_workflow aligns with the validations module in the lib folder which can be located as lib/validations.py. The library has components and functions that are callable and can be imported as many time as possible to maintain reusability for the etl_workflow phase





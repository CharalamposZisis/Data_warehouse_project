# Data_warehouse_project
Building a modern data warehouse with SQL Server, including ETL processes, data modeling and analytics.

Welcome to my **Data_warehouse_project** repository!
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. 

The Data architecture of this project follow an architecture of **Bronze** **Silver** and **Gold** layers:

![Data Warehouse Diagram](docs/DataWarehouse%20Diagram.png)

**Bronze Layer**: Stores raw data as is from the source systems. Data is ingested from csv files into sql server database.
**Silver Layer**: This layer includes data cleaning, standardization, and normalization processes to prepare data for analysis.
**Gold Layer**: In gold Layer the data are ready for analytics


# Data_warehouse_project
Building a modern data warehouse with SQL Server, including ETL processes, data modeling and analytics.

Welcome to my **Data_warehouse_project** repository!
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. 

The Data architecture of this project follow an architecture of **Bronze** **Silver** and **Gold** layers:

![Data Warehouse Diagram](docs/DataWarehouse%20Diagram.png)

**Bronze Layer**: Stores raw data as is from the source systems. Data is ingested from csv files into sql server database.<br />
**Silver Layer**: This layer includes data cleaning, standardization, and normalization processes to prepare data for analysis.<br />
**Gold Layer**: In gold Layer the data are ready for analytics

The current study represents my first step into Data Warehousing, where I gained valuable knowledge about ETL pipeline processes. However, one omission in this repository is that during the ETL process I did not include quality checks for each layer. In future work, I plan to place greater emphasis on implementing and tracking data quality at every stage of the pipeline.

Special Thanks to [@DataWithBaraa](https://github.com/DataWithBaraa) for his guidance. 

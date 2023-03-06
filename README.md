# multinational-retail-data-centralisation

## Summary

Collect data about a multinational retail company's operations from different sources (such as AWS RDS database, S3 storage, API, pdf file, json file and csv file) and clean & transform the data to suitable formats, in order to store them in a newly created local postgres database with a star based schema. This makes its sales data accessible from one centralised location and allows to analyse and get up to date metrics about the business

## Milestone 2

- Initialised a new **postgres** database, "Sales_Data", locally to store the extracted data
- Created three different classes:
  - DataExtractor : methods that help extract data from different sources
  - DatabaseConnector : methods to connect with and upload to a database
  - DataCleaning : methods to clean data from each of the sources
- Extracted users data from an **AWS** database, cleaned it and uploaded it as a table named 'dim_users' to local database

  - Connected to database using **sqlalchemy** library after reading the credentials from a **yaml** file which have the following details:

  ```
  RDS_HOST:
  RDS_PASSWORD:
  RDS_USER:
  RDS_DATABASE:
  RDS_PORT:
  ```

  - Read the data to a **pandas** dataframe and cleaned it

- Extracted credit/debit card data from a **pdf** file using **tabula-py** library, cleaned it and uploaded it as a table named 'dim_card_details' to local database
- Extracted stores data from an **API** using **requests** library, cleaned it and uploaded it as a table named 'dim_store_details' to local database
- Extracted products data from **csv** file stored in an S3 storage bucket using **boto3** library, cleaned it and uploaded it as a table named 'dim_products' to local database
- Extracted orders data from an AWS database, cleaned it and uploaded it as a table named 'orders_table' to local database. This table will act as source of truth for sales data and will be center of the star based schema of the local database
- Extraceted date events data from a **json** file, cleaned it and uploaded it as a table named 'dim_date_times' to local database

## Milestone 3

- Casted the columns of the different tables in the local database to required datatypes and made changes to a few tables, such as adding a column based on the other columns, using **SQL**
- Created the primary keys for all the tables other than orders_table in the local database
- Added foreign key constraints to the orders_table that reference the primary keys of the other table to complete the star based database schema

## Milestone 4

Queried the database using SQL to get some up-to-date metrics and information that will help make data driven business decisions

# Water-Quality-DW-on-Oracle-Database
This is an Oracle DB Data Warehouse and manual ETL demo on a specially formatted Water Quality dataset from DEFRA, UK. It is a personal academic-grade exercise to explore the basic concepts of data warehousing and manual ETL process from an academic perspective.

## Introduction:

A data warehouse is a central repository of information that can be analyzed to make more informed decisions. Data flows into a data warehouse from transactional systems, relational databases, and other sources, typically on a regular cadence (https://aws.amazon.com/what-is/data-warehouse).

This repository is about a data warehouse project that was carried out using a manually triggered ETL (extract, transform, and load) process on a [specially formatted WaterQuality dataset](https://github.com/vaxdata22/Water-Quality-DW-on-Oracle-Database/blob/main/WaterQuality.accdb) from The Department for Environment Food & Rural Affairs (DEFRA), UK. The particular dataset used for this project is provided in an MS Access (.accdb) file. It contains 17 tables, and each would have to be exported into individual CSV files.

The data warehouse consists of a staging table, six (6) dimension tables, and one fact table. Among the dimension tables is a Time table to aid time-based BI analysis. The data warehouse was implemented on Oracle Database 21c infrastructure with the source dataset exported into CSV files then imported into a raw table in the database; while the transform phase which involved data exploration and cleaning as well as the load phase was done using the Jupyter Notebook Python environment connected to the database.. 

Finally, SQL queries were run on the data warehouse star schema using the project questions to gain insights into the data.

## Objectives of the project:

These are the objectives of the project:

* To design a data warehouse on Oracle Database environment for the WaterQuality dataset to enable analysis.
* To implement ETL process and demonstrate its use cases especially in the extract phase.
* To demonstrate the use of PL/SQL cursors to populate the fact and dimension tables in the data warehouse.
* To implement a star schema for the data warehouse project.
* To demonstrate the use of Python connection to the database.

The following are information desired to be gotten from the dataset:

1. The list of water sensors measured by type of sensor by month
2. The number of sensor measurements collected by type of sensor by week
3. The number of measurements made by location by month
4. The average number of measurements covered for pH by year
5. The average value of Nitrate measurements by locations by year

## Deliverables on the project:

* Here is a [SQL file](https://github.com/vaxdata22/Water-Quality-DW-on-Oracle-Database/blob/main/Setup.%20PLSQL%20code%20to%20create%20the%20staging%20table.sql) containing PL/SQL command to create the staging table.
* Here is a [Jupyter Notebook](https://github.com/vaxdata22/Water-Quality-DW-on-Oracle-Database/blob/main/Python%20Environment%20To%20Demonstrate%20DW%20%26%20ETL%20on%20Oracle.ipynb) that provided a Python environment to carry out the data cleaning and ETL.
* Here are all the [SQL queries and PL/SQL codes](https://github.com/vaxdata22/Water-Quality-DW-on-Oracle-Database/tree/main/SQL%20queries%20and%20PLSQL%20codes) that were used at different times throughout the project.
* Extra bonus, a [TXT file](https://github.com/vaxdata22/Water-Quality-DW-on-Oracle-Database/blob/main/Bonus.%20Commands%20for%20dumping%20to%20DAT%20flat%20file%20and%20loading%20by%20SQL%20Loader.txt) containing code on how to dump a table into DAT flat file, and how to use SQL*Loader to load the DAT file into a table.

Enjoy!




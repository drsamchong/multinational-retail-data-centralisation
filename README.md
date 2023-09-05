## Multinational Retail Data Centralisation project

This project uses the scenario of being an employee of a multinational company that sells products internationally.</br></br>
There is a drive to centralise the sales data of the country to improve its accessibility by members of the global team and to ensure there is a single consistent and reliable source of data. This will facilitate analysis and promote evidence-driven business decisions.</br>


### Technologies


The project uses python 3.11 as the main language for its codebase. Many Python libraries are available which make interfaces with APIs and relational databases straighforward, in addition to package for data manipulation, analysis and visualisation.


In this projects the main libraries used are

- pandas 2.0 - for data analysis and manipulation
- SQLAlchemy - for interaction with PostgreSQL databases (using the psycopg2 adapter)
- numpy - for manipulation and analysis of data


### Structure

The code consists of three classes which are contained in separate python files


```DatabaseConnector```
- ```database_utils.py``` 
- provides utility methods to make connections and interact with a PostgreSQL database


```DataExtractor``` 
- ```data_extraction.py```
- contains methods for extracting data from sources

```DataCleaner```
- ```data_cleaning.py```
- contains methods for pre-processing data in a Pandas dataframe


A driver script ```script.py``` is used to run the code, which imports the classes and the Pandas library.

</br>
The project is a work in progress (as of 01/09/2023)...
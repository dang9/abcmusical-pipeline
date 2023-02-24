# Readme

## Code

The code is a Python script that performs ETL operations on CSV files located in a folder. The script extracts data from CSV files, transforms the data, and then loads the transformed data into a SQLite database.

The following are the functions implemented in the code:

- `get_csv_files_for_processing`: This function takes a folder path as an argument and returns a list of CSV files located in the folder.

- `extract_file`: This function takes a file path as an argument and reads the CSV file with the appropriate schema. The function returns a Pandas dataframe.

- `extract_all_files`: This function takes a list of file paths as an argument and calls `extract_file` on each file path. The function then concatenates the resulting dataframes into a single dataframe.

- `transform_data`: This function performs the necessary data transformations on the input dataframe. The function adds a `LoadDate` column, a `RecordId` column, converts the `PaymentDate` column to a datetime type, and converts the `ClientName` column to lowercase.

- `create_dimension_client`: This function creates the `dim_client` dataframe by selecting the appropriate columns from the input dataframe and dropping any duplicate rows.

- `create_dimension_payment`: This function creates the `dim_payment` dataframe by selecting the appropriate columns from the input dataframe and dropping any duplicate rows.

- `create_dimension_product`: This function creates the `dim_product` dataframe by selecting the appropriate columns from the input dataframe and dropping any duplicate rows.

- `create_fact_orders`: This function creates the `fact_orders` dataframe by selecting the appropriate columns from the input dataframe and dropping any duplicate rows.

- `move_processed_files`: This function moves processed files from the `unprocessed` folder to the `processed` folder.

## Usage

The script expects the following folder structure:

```
/
|-- src/
| |-- etl.py
| |-- db_helper.py
| |-- main.py
| |-- logger.py
|-- data/
| |-- unprocessed/
| |-- processed/
| | |-- data.csv
|-- tests/
| |-- test_etl.py
```


To use the script, do the following:

1. Clone the repository.

2. Place CSV files to be processed in the `unprocessed` folder located in the `data` folder.

3. Run the `main.py` script.

4. The script will extract data from the CSV files, transform the data, load the data into the SQLite database, and move processed files to the `processed` folder located in the `data` folder.

Note: The script assumes that you have installed the necessary dependencies, which include `pandas`, `numpy`, `pendulum`, and `sqlite3`.
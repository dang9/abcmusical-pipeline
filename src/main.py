from lib.etl import get_csv_files_for_processing, extract_all_files, extract_file, transform_data, create_dimension_client, create_dimension_payment, create_dimension_product, create_fact_orders, move_processed_files
from lib.db_helper import create_connection, create_tables_in_db, load_data_clients, load_data_payments, load_data_products, load_data_orders
from lib.logger import setup_logging
import os
import logging

def main():
    """
    The main function of the ETL pipeline for the ABC Musical Data Warehouse project.

    This function extracts data from CSV files in a specified directory, transforms the data,
    creates dimension and fact tables, and loads the data into a SQLite database.

    Returns:
        None
    """
    # Set up logging
    setup_logging()

    # Set the path to the SQLite database
    database = r".\output\abcmusicaldwh.db"

    # Create a connection to the database
    create_connection(database)

    # Create the necessary tables in the database
    create_tables_in_db(database, r".\model\create_tables.sql")

    # Get a list of CSV files to process
    files = get_csv_files_for_processing(r".\data\unprocessed")

    # exit if no files exist to process
    if len(files) == 0:
        logging.error("No files to process, please place files in data/unprocessed", exc_info=True)
        exit()

    # Extract data from all CSV files
    raw_df = extract_all_files(files)

    # Transform the data
    transformed_df = transform_data(raw_df)

    # Create dimension tables
    dim_client_df = create_dimension_client(transformed_df)
    dim_payment_df = create_dimension_payment(transformed_df)
    dim_product_df = create_dimension_product(transformed_df)

    # Convert the PaymentDate column to a string
    dim_payment_df["PaymentDate"] = dim_payment_df["PaymentDate"].astype(str)

    # Load the dimension tables into the database
    load_data_clients(database, dim_client_df)
    load_data_payments(database, dim_payment_df)
    load_data_products(database, dim_product_df)

    # Create the fact table with foreign keys to dimension tables
    fact_orders_df = create_fact_orders(database, transformed_df)

    # Load the fact table into the database
    load_data_orders(database, fact_orders_df)

    # Move processed files to the processed folder
    move_processed_files(r".\data\unprocessed", r".\data\processed",)


if __name__ == '__main__':
    main()

import pandas as pd
import numpy as np
import pendulum
from pathlib import Path
import shutil
import os
import logging
import sqlite3
from typing import List


def get_csv_files_for_processing(folder_path: str) -> List[Path]:
    """Get a list of CSV files in a folder.

    Args:
        folder_path: A string representing the folder path.

    Returns:
        A list of Path objects representing the CSV files in the folder.
    """
    try:
        files = Path(folder_path).glob("*.csv")
        return list(files)
    except Exception as e:
        logging.error(f"Error while getting CSV files: {e}", exc_info=True)
        return []


def extract_file(file_path: Path) -> pd.DataFrame:
    """Extract data from a CSV file.

    Args:
        file_path: A Path object representing the file path.

    Returns:
        A pandas DataFrame containing the extracted data, or None if there was an error.
    """
    input_schema = {
        "OrderNumber": str,
        "ClientName": str,
        "ProductName": str,
        "ProductType": str,
        "UnitPrice": float,
        "ProductQuantity": "int64",
        "TotalPrice": float,
        "Currency": str,
        "DeliveryAddress": str,
        "DeliveryCity": str,
        "DeliveryPostcode": str,
        "DeliveryCountry": str,
        "DeliveryContactNumber": str,
        "PaymentType": str,
        "PaymentBillingCode": str,
        "PaymentDate": str,
    }

    try:
        extracted_df = pd.read_csv(
            file_path,
            dtype=input_schema,
            encoding="unicode_escape",
            header=[0],
            on_bad_lines="skip",
        )
        return extracted_df
    except Exception as e:
        logging.error(
            f"Error while extracting data from {file_path}: {e}", exc_info=True
        )
        return None


def extract_all_files(files: List[Path]) -> pd.DataFrame:
    """Extract data from multiple CSV files and concatenate into a single DataFrame.

    Args:
        files: A list of Path objects representing the CSV files to extract data from.

    Returns:
        A pandas DataFrame containing the extracted data from all the files, or None if no data was extracted.
    """
    dfs = []
    for f in files:
        data = extract_file(f)
        if data is not None:
            data["file"] = f.stem
            dfs.append(data)

    if len(dfs) == 0:
        logging.warning("No data extracted from CSV files")
        return None

    df = pd.concat(dfs, ignore_index=True)
    return df


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    """Transform the input DataFrame by adding columns and modifying data.

    Args:
        input_df: A pandas DataFrame representing the input data.

    Returns:
        A pandas DataFrame with the transformed data.
    """
    # add a new column 'LoadDate' with the current date and time in UTC
    input_df["LoadDate"] = pd.to_datetime(pendulum.now(tz="UTC").to_iso8601_string())

    # add a new column 'RecordId' with unique ID for each row
    input_df["RecordId"] = np.arange(len(input_df))

    # convert 'PaymentDate' column to datetime datatype
    input_df["PaymentDate"] = pd.to_datetime(input_df["PaymentDate"])

    # convert 'ClientName' column to lowercase
    input_df["ClientName"] = input_df["ClientName"].str.lower()

    # return the transformed DataFrame
    return input_df


def create_dimension_client(input_df: pd.DataFrame) -> pd.DataFrame:
    """Create a dimension table for clients from the input data.

    Args:
        input_df: A pandas DataFrame representing the input data.

    Returns:
        A pandas DataFrame representing the dimension table for clients.
    """
    # select columns for dimension table
    dim_client_columns = [
        "RecordId",
        "OrderNumber",
        "ClientName",
        "DeliveryAddress",
        "DeliveryCity",
        "DeliveryPostcode",
        "DeliveryCountry",
        "DeliveryContactNumber",
    ]

    # drop duplicates to create unique rows
    dim_client_df = input_df[dim_client_columns].drop_duplicates(
        [
            "ClientName",
            "DeliveryAddress",
            "DeliveryCity",
            "DeliveryPostcode",
            "DeliveryCountry",
            "DeliveryContactNumber",
        ]
    )

    # print the shape of the resulting DataFrame
    print(dim_client_df.shape)

    return dim_client_df


def create_dimension_payment(input_df: pd.DataFrame) -> pd.DataFrame:
    """Create a dimension table for payment from the input data.

    Args:
        input_df: A pandas DataFrame representing the input data.

    Returns:
        A pandas DataFrame representing the dimension table for payment.
    """
    # select columns for dimension table
    dim_payment_columns = [
        "RecordId",
        "OrderNumber",
        "PaymentType",
        "PaymentBillingCode",
        "PaymentDate",
        "Currency",
    ]

    # drop duplicates to create unique rows
    dim_payment_df = input_df[dim_payment_columns].drop_duplicates(
        ["PaymentBillingCode"]
    )

    # print the shape of the resulting DataFrame
    print(dim_payment_df.shape)

    return dim_payment_df


def create_dimension_product(input_df: pd.DataFrame) -> pd.DataFrame:
    """Create a dimension table for products from the input data.

    Args:
        input_df: A pandas DataFrame representing the input data.

    Returns:
        A pandas DataFrame representing the dimension table for products.
    """
    # select columns for dimension table
    dim_product_columns = ["RecordId", "OrderNumber", "ProductName", "ProductType"]

    # drop duplicates to create unique rows
    dim_product_df = input_df[dim_product_columns].drop_duplicates(
        ["ProductName", "ProductType"]
    )

    # print the shape of the resulting DataFrame
    print(dim_product_df.shape)

    return dim_product_df


def create_fact_orders(database: str, input_df: pd.DataFrame) -> pd.DataFrame:
    """Create a fact table for orders by joining the input data with dimension tables.

    Args:
        input_df: A pandas DataFrame representing the input data.

    Returns:
        A pandas DataFrame representing the fact table for orders.
    """
    # connect to SQLite database
    conn = sqlite3.connect(database)

    # query dimension tables for client, product, and payment keys
    dim_clients_query = """SELECT * FROM dim_clients"""
    dim_client_df_keys = pd.read_sql_query(dim_clients_query, conn)

    dim_product_query = """SELECT * FROM dim_products"""
    dim_product_df_keys = pd.read_sql_query(dim_product_query, conn)

    dim_payment_query = """SELECT * FROM dim_payment"""
    dim_payment_df_keys = pd.read_sql_query(dim_payment_query, conn)

    # merge input data with dimension tables using keys and keep desired columns
    columns_to_keep = input_df.columns.tolist() + [
        "client_key",
        "product_key",
        "payment_key",
    ]
    fact_orders_with_keys = (
        input_df.merge(
            dim_client_df_keys,
            left_on="ClientName",
            right_on="client_name",
            how="inner",
        )
        .merge(
            dim_product_df_keys,
            left_on="ProductName",
            right_on="product_name",
            how="inner",
        )
        .merge(
            dim_payment_df_keys,
            left_on="PaymentBillingCode",
            right_on="payment_billing_code",
            how="inner",
        )
    )[columns_to_keep]

    # select columns for fact table and drop duplicates to create unique rows
    fact_orders_columns = [
        "RecordId",
        "OrderNumber",
        "client_key",
        "product_key",
        "payment_key",
        "UnitPrice",
        "ProductQuantity",
        "TotalPrice",
    ]
    fact_orders_df = fact_orders_with_keys[fact_orders_columns].drop_duplicates(
        ["OrderNumber", "UnitPrice", "ProductQuantity", "TotalPrice"]
    )

    # print the shape of the resulting DataFrame
    print(fact_orders_df.shape)
    
    conn.close()

    return fact_orders_df


def move_processed_files(
    src: str = r"C:\Users\Anthony\Documents\GitHub\allicabank\data\unprocessed",
    dest: str = r"C:\Users\Anthony\Documents\GitHub\allicabank\data\processed",
) -> None:
    """Move processed files from the source folder to the destination folder.

    Args:
        src: A string representing the path to the source folder.
        dest: A string representing the path to the destination folder.

    Returns:
        None.
    """
    try:
        # loop over files in the source folder
        for item in os.listdir(src):
            print(f"Moving {item}...")
            # move each file to the destination folder
            shutil.move(os.path.join(src, item), os.path.join(dest, item))
    except OSError as e:
        print(f"Error while moving files: {e}")
    except Exception as e:
        print(f"Unknown error: {e}")
    else:
        print("All files moved successfully.")

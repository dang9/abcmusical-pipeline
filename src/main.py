from lib.etl import get_csv_files_for_processing, extract_all_files, extract_file, transform_data, create_dimension_client, create_dimension_payment, create_dimension_product, create_fact_orders, move_processed_files
from lib.db_helper import create_connection, create_tables_in_db, load_data_clients, load_data_payments, load_data_products, load_data_orders
from lib.logger import setup_logging
import os

def main():
    setup_logging()
    print(os.getcwd())
    database = r".\output\abcmusicaldwh.db"

    create_connection(database)

    create_tables_in_db(database, r".\model\create_tables.sql")

    files = get_csv_files_for_processing(r".\data\unprocessed")

    raw_df = extract_all_files(files)

    transformed_df = transform_data(raw_df)

    # create dimensions
    dim_client_df = create_dimension_client(transformed_df)
    dim_payment_df = create_dimension_payment(transformed_df)
    dim_product_df = create_dimension_product(transformed_df)

    dim_payment_df["PaymentDate"] = dim_payment_df["PaymentDate"].astype(str)

    load_data_clients(database, dim_client_df)
    load_data_payments(database, dim_payment_df)
    load_data_products(database, dim_product_df)

    # create facts
    fact_orders_df = create_fact_orders(database, transformed_df)

    load_data_orders(database, fact_orders_df)

    move_processed_files(r".\data\unprocessed", r".\data\processed",)


if __name__ == '__main__':
    main()

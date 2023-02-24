import sqlite3
import logging
import pandas as pd


def create_connection(db_file):
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def create_tables_in_db(database, sql_path):
    conn = None
    try:
        conn = sqlite3.connect(database)
        cur = conn.cursor()
        # read SQL query from file
        with open(sql_path, "r") as f:  
            sql_query = f.read()
            # execute query
        cur.executescript(sql_query)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
def create_connection_for_load(database, insert_query, insert_data):
    conn = None
    try:
        conn = sqlite3.connect(database)
        cur = conn.cursor()
        cur.executemany(insert_query, insert_data)
        conn.commit()
    except Exception as e:
        logging.error(f"Error while loading client data: {e}", exc_info=True)
        raise e
    finally:
        if conn:
            conn.close()

def load_data_products(database: str, input_df: pd.DataFrame) -> None:
    """Load product data into the products dimension table.

    Args:
        conn: A SQLite3 database connection object.
        input_df: A pandas DataFrame representing the input data.

    Returns:
        None.
    """
    insert_query = """
    INSERT into dim_products (product_name, product_type)
    values(?, ?)
    ON CONFLICT(product_name) DO UPDATE SET product_type=excluded.product_type;
    """

    insert_data = list(
        input_df[["ProductName", "ProductType"]].itertuples(index=False, name=None)
    )
    
    create_connection_for_load(database, insert_query, insert_data)

def load_data_clients(database: str, input_df: pd.DataFrame) -> None:
    """Load client data into the clients dimension table.

    Args:
        conn: A SQLite3 database connection object.
        input_df: A pandas DataFrame representing the input data.

    Returns:
        None.
    """
    insert_query = """
        INSERT into dim_clients (client_name, delivery_address, delivery_city, delivery_postcode, delivery_country, delivery_contact_number)
        values(?, ?, ?, ?, ?, ?)
        ON CONFLICT(client_name) DO UPDATE 
        SET delivery_address=excluded.delivery_address
    """

    insert_data = list(
        input_df[
            [
                "ClientName",
                "DeliveryAddress",
                "DeliveryCity",
                "DeliveryPostcode",
                "DeliveryCountry",
                "DeliveryContactNumber",
            ]
        ].itertuples(index=False, name=None)
    )

    create_connection_for_load(database, insert_query, insert_data)


def load_data_payments(database: str, input_df: pd.DataFrame) -> None:
    """Load payment data into the payment dimension table.

    Args:
        conn: A SQLite3 database connection object.
        input_df: A pandas DataFrame representing the input data.

    Returns:
        None.
    """
    insert_query = """
    INSERT into dim_payment (payment_date, payment_type, payment_billing_code, currency)
    values(?, ?, ?, ?)
    ON CONFLICT(payment_billing_code) DO NOTHING;
    """

    insert_data = list(
        input_df[
            ["PaymentDate", "PaymentType", "PaymentBillingCode", "Currency"]
        ].itertuples(index=False, name=None)
    )
    
    create_connection_for_load(database, insert_query, insert_data)


def load_data_orders(database: str, input_df: pd.DataFrame) -> None:
    """Load order data into the fact_orders table.

    Args:
        conn: A SQLite3 database connection object.
        input_df: A pandas DataFrame representing the input data.

    Returns:
        None.
    """
    insert_query = """
    INSERT into fact_orders (order_number, client_key, product_key, payment_key, unit_price, product_quantity, total_price)
    values(?, ?, ?, ?, ?, ?, ?);
    """

    insert_data = list(
        input_df[
            [
                "OrderNumber",
                "client_key",
                "product_key",
                "payment_key",
                "UnitPrice",
                "ProductQuantity",
                "TotalPrice",
            ]
        ].itertuples(index=False, name=None)
    )
    
    create_connection_for_load(database, insert_query, insert_data)

import pandas as pd
import numpy as np


from src.lib.etl import get_csv_files_for_processing, extract_file, transform_data, create_dimension_client, create_dimension_payment, create_dimension_product, create_fact_orders

def test_get_csv_files_for_processing():
    folder_path = "./test_data/"
    expected_files = ["file1.csv", "file2.csv"]
    files = get_csv_files_for_processing(folder_path)
    assert [str(f.stem)+f.suffix for f in files] == expected_files
    
def test_extract_file():
    file_path = "./test_data/file1.csv"
    expected_df = pd.DataFrame({
        "OrderNumber": ["1001", "1002", "1003"],
        "ClientName": ["John Smith", "Mary Brown", "Bob Johnson"],
        "ProductName": ["Widget A", "Widget B", "Widget C"],
        "ProductType": ["Small", "Medium", "Large"],
        "UnitPrice": [1.99, 2.99, 3.99],
        "ProductQuantity": [10, 20, 30],
        "TotalPrice": [19.90, 59.80, 119.70],
        "Currency": ["USD", "USD", "USD"],
        "DeliveryAddress": ["123 Main St", "456 Elm St", "789 Oak St"],
        "DeliveryCity": ["Anytown", "Otherville", "Myville"],
        "DeliveryPostcode": ["12345", "67890", "24680"],
        "DeliveryCountry": ["USA", "USA", "USA"],
        "DeliveryContactNumber": ["123-456-7890", "987-654-3210", "555-1212"]
    })
    result_df = extract_file(file_path)
    pd.testing.assert_frame_equal(result_df, expected_df)
    
def test_transform_data():
    # create test input dataframe
    input_data = {'PaymentDate': ['2022-01-01', '2022-01-02', '2022-01-03'],
                  'ClientName': ['John', 'Mary', 'David'],
                  'OrderNumber': ['ORD001', 'ORD002', 'ORD003'],
                  'DeliveryAddress': ['123 Main St', '456 Elm St', '789 Oak St'],
                  'DeliveryCity': ['New York', 'Los Angeles', 'Chicago'],
                  'DeliveryPostcode': ['10001', '90001', '60601'],
                  'DeliveryCountry': ['USA', 'USA', 'USA'],
                  'DeliveryContactNumber': ['111-111-1111', '222-222-2222', '333-333-3333'],
                  'UnitPrice': [10.0, 20.0, 30.0],
                  'ProductQuantity': [1, 2, 3],
                  'TotalPrice': [10.0, 40.0, 90.0]}
    input_df = pd.DataFrame(data=input_data)
    
    # call transform_data function
    output_df = transform_data(input_df)
    
    # create expected output dataframe
    expected_data = {'PaymentDate': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03']),
                     'ClientName': ['john', 'mary', 'david'],
                     'OrderNumber': ['ORD001', 'ORD002', 'ORD003'],
                     'DeliveryAddress': ['123 Main St', '456 Elm St', '789 Oak St'],
                     'DeliveryCity': ['New York', 'Los Angeles', 'Chicago'],
                     'DeliveryPostcode': ['10001', '90001', '60601'],
                     'DeliveryCountry': ['USA', 'USA', 'USA'],
                     'DeliveryContactNumber': ['111-111-1111', '222-222-2222', '333-333-3333'],
                     'UnitPrice': [10.0, 20.0, 30.0],
                     'ProductQuantity': [1, 2, 3],
                     'TotalPrice': [10.0, 40.0, 90.0],
                     'LoadDate': output_df['LoadDate'], # how to match load datetime in test?
                     'RecordId': np.arange(len(input_df))}
    expected_df = pd.DataFrame(data=expected_data)
    
    # assert that output dataframe equals expected dataframe
    pd.testing.assert_frame_equal(output_df, expected_df)
    

# test data
input_df = pd.DataFrame({
    "RecordId": [1, 2, 3, 4],
    "OrderNumber": ["A1", "A2", "B1", "B2"],
    "ClientName": ["John Doe", "Jane Doe", "John Doe", "Mary Smith"],
    "DeliveryAddress": ["123 Main St", "456 Elm St", "123 Main St", "789 Oak St"],
    "DeliveryCity": ["Anytown", "Othertown", "Anytown", "Sometown"],
    "DeliveryPostcode": ["12345", "67890", "12345", "45678"],
    "DeliveryCountry": ["USA", "Canada", "USA", "Mexico"],
    "DeliveryContactNumber": ["555-1234", "555-5678", "555-1234", "555-9012"],
    "PaymentType": ["Credit Card", "Debit Card", "Credit Card", "PayPal"],
    "PaymentBillingCode": ["123456", "789012", "345678", "901234"],
    "PaymentDate": ["2022-01-01", "2022-01-02", "2022-01-01", "2022-01-02"],
    "Currency": ["USD", "CAD", "USD", "MXN"],
    "ProductName": ["Product A", "Product B", "Product A", "Product C"],
    "ProductType": ["Type 1", "Type 2", "Type 1", "Type 3"],
    "UnitPrice": [10.00, 20.00, 10.00, 30.00],
    "ProductQuantity": [2, 1, 3, 1],
    "TotalPrice": [20.00, 20.00, 30.00, 30.00]
})

def test_create_dimension_client():
    # test that the function returns the correct number of rows
    assert len(create_dimension_client(input_df)) == 3
    
    # test that the function returns the expected column names
    expected_columns = ["RecordId", "OrderNumber", "ClientName", "DeliveryAddress", "DeliveryCity", "DeliveryPostcode","DeliveryCountry","DeliveryContactNumber"]
    assert set(create_dimension_client(input_df).columns) == set(expected_columns)
    
def test_create_dimension_payment():
    # test that the function returns the correct number of rows
    assert len(create_dimension_payment(input_df)) == 4
    
    # test that the function returns the expected column names
    expected_columns = ["RecordId", "OrderNumber", "PaymentType", "PaymentBillingCode", "PaymentDate", "Currency"]
    assert set(create_dimension_payment(input_df).columns) == set(expected_columns)
    
def test_create_dimension_product():
    # test that the function returns the correct number of rows
    assert len(create_dimension_product(input_df)) == 3
    
    # test that the function returns the expected column names
    expected_columns = ["RecordId", "OrderNumber", "ProductName", "ProductType"]
    assert set(create_dimension_product(input_df).columns) == set(expected_columns)
    
# def test_create_fact_orders():
#     # test that the function returns the correct number of rows
#     assert len(create_fact_orders(input_df)) == 4
    
#     # test that the function returns the expected column names
#     expected_columns = ["RecordId", "OrderNumber", "UnitPrice", "ProductQuantity", "TotalPrice"]
#     assert set(create_fact_orders(input_df).columns) == set(expected_columns)
    
DROP TABLE IF EXISTS dim_clients;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_payment;
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_clients_history;

CREATE TABLE IF NOT EXISTS dim_clients (
  client_key INTEGER NOT NULL PRIMARY KEY,
  client_name VARCHAR(255) NOT NULL UNIQUE,
  delivery_address VARCHAR(255) NOT NULL,
  delivery_city VARCHAR(255) NOT NULL,
  delivery_postcode VARCHAR(255) NOT NULL,
  delivery_country VARCHAR(255) NOT NULL,
  delivery_contact_number VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_products (
  product_key INTEGER NOT NULL PRIMARY KEY,
  product_name VARCHAR(255) NOT NULL UNIQUE,
  product_type VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_payment (
  payment_key INTEGER NOT NULL PRIMARY KEY,
  payment_date DATETIME NOT NULL,
  payment_type VARCHAR(255) NOT NULL,
  payment_billing_code VARCHAR(255) NOT NULL UNIQUE,
  currency VARCHAR(255) NOT NULL
);

-- Create the fact table
CREATE TABLE IF NOT EXISTS fact_orders (
  order_number VARCHAR(255) NOT NULL PRIMARY KEY,
  client_key INTEGER NOT NULL,
  product_key INTEGER NOT NULL,
  payment_key INTEGER NOT NULL,
  unit_price DECIMAL(10, 2) NOT NULL,
  product_quantity INT NOT NULL,
  total_price DECIMAL(10, 2) NOT NULL,
  FOREIGN KEY (client_key) REFERENCES dim_clients(client_key),
  FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
  FOREIGN KEY (payment_key) REFERENCES dim_payment(payment_key)
);


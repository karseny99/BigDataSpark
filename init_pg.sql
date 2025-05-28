CREATE TABLE mock_data (
    id INT,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INT,
    customer_email VARCHAR(100),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(20),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(100),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(20),
    product_name VARCHAR(100),
    product_category VARCHAR(100),
    product_price DECIMAL(10,2),
    product_quantity INT,
    sale_date DATE,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price DECIMAL(10,2),
    store_name VARCHAR(100),
    store_location VARCHAR(100),
    store_city VARCHAR(100),
    store_state VARCHAR(20),
    store_country VARCHAR(100),
    store_phone VARCHAR(20),
    store_email VARCHAR(100),
    pet_category VARCHAR(50),
    product_weight DECIMAL(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(20),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating DECIMAL(3,1),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(100),
    supplier_contact VARCHAR(100),
    supplier_email VARCHAR(100),
    supplier_phone VARCHAR(20),
    supplier_address VARCHAR(200),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);
COPY mock_data
  FROM '/исходные данные/MOCK_DATA.csv' WITH (FORMAT csv, HEADER true);
COPY mock_data
  FROM '/исходные данные/MOCK_DATA (1).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (2).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (3).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (4).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (5).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (6).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (7).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (8).csv' 
  WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/исходные данные/MOCK_DATA (9).csv' 
  WITH (FORMAT csv, HEADER true);


CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id INT UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    age INT,
    email VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_date (
    date_sk SERIAL PRIMARY KEY,
    sale_date DATE UNIQUE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    weekday INT
);

CREATE TABLE dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id INT UNIQUE,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(100),
    weight DECIMAL(10,2),
    color VARCHAR(50),
    size VARCHAR(20),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating DECIMAL(3,1),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    unit_price DECIMAL(10,2)
);

CREATE TABLE dim_seller (
    seller_sk SERIAL PRIMARY KEY,
    seller_id INT UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_store (
    store_sk SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE,
    location VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(20),
    country VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(100)
);

CREATE TABLE dim_supplier (
    supplier_sk SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE,
    contact VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE fact_sales (
    sale_sk SERIAL PRIMARY KEY,
    date_sk INT NOT NULL REFERENCES dim_date(date_sk),
    customer_sk INT NOT NULL REFERENCES dim_customer(customer_sk),
    seller_sk INT NOT NULL REFERENCES dim_seller(seller_sk),
    product_sk INT NOT NULL REFERENCES dim_product(product_sk),
    store_sk INT NOT NULL REFERENCES dim_store(store_sk),
    supplier_sk INT NOT NULL REFERENCES dim_supplier(supplier_sk),
    sale_quantity INT,
    sale_total_price DECIMAL(10,2),
    unit_price DECIMAL(10,2)
);
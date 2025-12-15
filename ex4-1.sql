DROP TABLE IF EXISTS ex_4_fact_sales;
DROP TABLE IF EXISTS ex_4_dim_products;
DROP TABLE IF EXISTS ex_4_dim_customers;


CREATE TABLE ex_4_dim_products (
    product_key     INT AUTO_INCREMENT PRIMARY KEY,
    product_id      VARCHAR(10) NOT NULL,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(50) NOT NULL,
    price           DECIMAL(10,2),

    UNIQUE KEY uk_ex4_product_id (product_id)
);


CREATE TABLE ex_4_dim_customers (
    customer_key    INT AUTO_INCREMENT PRIMARY KEY,
    customer_id     VARCHAR(10) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    email           VARCHAR(255),
    country         VARCHAR(100),

    UNIQUE KEY uk_ex4_customer_id (customer_id)
);


CREATE TABLE ex_4_fact_sales (
    sales_key           BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id      VARCHAR(20) NOT NULL,
    product_key         INT NOT NULL,
    customer_key        INT NOT NULL,
    quantity            INT NOT NULL,
    unit_price          DECIMAL(10,2),
    transaction_date    DATE NOT NULL,


    CONSTRAINT fk_ex4_sales_product
        FOREIGN KEY (product_key) REFERENCES ex_4_dim_products(product_key),
    CONSTRAINT fk_ex4_sales_customer
        FOREIGN KEY (customer_key) REFERENCES ex_4_dim_customers(customer_key)
);


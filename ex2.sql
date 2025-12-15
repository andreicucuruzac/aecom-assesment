DROP TABLE IF EXISTS ex_2_sales;
DROP TABLE IF EXISTS ex_2_customers;
DROP TABLE IF EXISTS ex_2_products;

CREATE TABLE ex_2_products (
    product_id      VARCHAR(10),
    product_name    VARCHAR(255),
    category        VARCHAR(50),
    price           DECIMAL(10,2)
);

CREATE TABLE ex_2_customers (
    customer_id     VARCHAR(10),
    name            VARCHAR(255),
    email           VARCHAR(255),
    country         VARCHAR(100)
);

CREATE TABLE ex_2_sales (
    transaction_id      VARCHAR(20),
    product_id          VARCHAR(10),
    customer_id         VARCHAR(10),
    quantity            INT,
    transaction_date    DATE
);

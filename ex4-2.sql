INSERT INTO ex_4_dim_products (product_id, product_name, category, price)
SELECT DISTINCT
    product_id,
    product_name,
    category,
    price
FROM ex_2_products;

INSERT INTO ex_4_dim_customers (customer_id, name, email, country)
SELECT DISTINCT
    customer_id,
    name,
    email,
    country
FROM ex_2_customers;

INSERT INTO ex_4_fact_sales (
    transaction_id,
    product_key,
    customer_key,
    quantity,
    unit_price,
    transaction_date
)
SELECT
    s.transaction_id,
    dp.product_key,
    dc.customer_key,
    s.quantity,
    dp.price AS unit_price,
    s.transaction_date
FROM ex_2_sales s
JOIN ex_4_dim_products dp
    ON s.product_id = dp.product_id
JOIN ex_4_dim_customers dc
    ON s.customer_id = dc.customer_id;


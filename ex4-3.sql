CREATE INDEX idx_ex4_fact_product ON ex_4_fact_sales (product_key);


CREATE INDEX idx_ex4_fact_txdate_customer
    ON ex_4_fact_sales (transaction_date, customer_key);


CREATE INDEX idx_ex4_fact_txdate_product
    ON ex_4_fact_sales (transaction_date, product_key);
   
   
SELECT
    p.product_id,
    p.product_name,
    SUM(f.quantity) AS total_quantity
FROM ex_4_fact_sales f
JOIN ex_4_dim_products p
    ON f.product_key = p.product_key
GROUP BY
    p.product_id,
    p.product_name
ORDER BY
    total_quantity DESC
LIMIT 10;


SELECT
    c.customer_id,
    c.name,
    COUNT(*) AS num_purchases
FROM ex_4_fact_sales f
JOIN ex_4_dim_customers c
    ON f.customer_key = c.customer_key
WHERE
    f.transaction_date >= CURDATE() - INTERVAL 720 DAY
GROUP BY
    c.customer_id,
    c.name
HAVING
    COUNT(*) > 5
ORDER BY
    num_purchases DESC;
   

SELECT
    DATE_FORMAT(f.transaction_date, '%Y-%m-01') AS month_start,
    p.category,
    SUM(f.quantity * f.unit_price) AS total_revenue
FROM ex_4_fact_sales f
JOIN ex_4_dim_products p
    ON f.product_key = p.product_key
GROUP BY
    month_start,
    p.category
ORDER BY
    month_start,
    p.category;
   
   
SELECT
    YEAR(f.transaction_date) AS year,
    MONTH(f.transaction_date) AS month,
    p.category,
    SUM(f.quantity * f.unit_price) AS total_revenue
FROM ex_4_fact_sales f
JOIN ex_4_dim_products p
    ON f.product_key = p.product_key
GROUP BY
    year,
    month,
    p.category
ORDER BY
    year,
    month,
    p.category;
   
  






CREATE TABLE IF NOT EXISTS raw_sales (
    order_id INT,
    customer_id INT,
    product_name VARCHAR(100),
    region VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10,2),
    order_date VARCHAR(30),   -- intentionally VARCHAR: inconsistent formats
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

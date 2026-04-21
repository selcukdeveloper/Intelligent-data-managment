INSERT INTO dim_date (month, month_name, year)
SELECT DISTINCT
    EXTRACT(MONTH FROM order_date)::INT                    AS month,
    TO_CHAR(order_date, 'Month')                           AS month_name,
    EXTRACT(YEAR  FROM order_date)::INT                    AS year
FROM   stg_superstore
ON CONFLICT (month, year) DO NOTHING;

INSERT INTO dim_product (product_name, category_name)
SELECT DISTINCT product_name, category
FROM   stg_superstore;

INSERT INTO dim_customer (customer_name)
SELECT DISTINCT customer_name
FROM   stg_superstore;

INSERT INTO dim_location (state, region)
SELECT DISTINCT state, region
FROM   stg_superstore
ON CONFLICT (state, region) DO NOTHING;

INSERT INTO dim_ship_mode (ship_mode_name)
SELECT DISTINCT ship_mode
FROM   stg_superstore
ON CONFLICT (ship_mode_name) DO NOTHING;

INSERT INTO fact_sales (
    date_key, product_key, customer_key,
    location_key, ship_mode_key,
    sales_amount, quantity, profit_amount
)
SELECT
    d.date_key,
    p.product_key,
    c.customer_key,
    l.location_key,
    s.ship_mode_key,
    stg.sales,
    stg.quantity,
    stg.profit
FROM   stg_superstore stg
JOIN   dim_date      d ON d.year  = EXTRACT(YEAR  FROM stg.order_date)::INT
                      AND d.month = EXTRACT(MONTH FROM stg.order_date)::INT
JOIN   dim_product   p ON p.product_name = stg.product_name
                      AND p.category_name = stg.category
JOIN   dim_customer  c ON c.customer_name = stg.customer_name
JOIN   dim_location  l ON l.state = stg.state AND l.region = stg.region
JOIN   dim_ship_mode s ON s.ship_mode_name = stg.ship_mode;

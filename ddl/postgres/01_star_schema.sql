CREATE TABLE dim_date (
    date_key    SERIAL PRIMARY KEY,
    month       INT         NOT NULL,
    month_name  VARCHAR(20) NOT NULL,
    year        INT         NOT NULL,
    UNIQUE (month, year)
);

CREATE TABLE dim_product (
    product_key   SERIAL PRIMARY KEY,
    product_name  TEXT        NOT NULL,
    category_name VARCHAR(50) NOT NULL
);

CREATE TABLE dim_customer (
    customer_key  SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL
);

CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    state        VARCHAR(50) NOT NULL,
    region       VARCHAR(20) NOT NULL,
    UNIQUE (state, region)
);

CREATE TABLE dim_ship_mode (
    ship_mode_key  SERIAL PRIMARY KEY,
    ship_mode_name VARCHAR(30) NOT NULL UNIQUE
);

CREATE TABLE fact_sales (
    sales_fact_id  SERIAL PRIMARY KEY,
    date_key       INT     NOT NULL REFERENCES dim_date(date_key),
    product_key    INT     NOT NULL REFERENCES dim_product(product_key),
    customer_key   INT     NOT NULL REFERENCES dim_customer(customer_key),
    location_key   INT     NOT NULL REFERENCES dim_location(location_key),
    ship_mode_key  INT     NOT NULL REFERENCES dim_ship_mode(ship_mode_key),
    sales_amount   NUMERIC NOT NULL,
    quantity       INT     NOT NULL,
    profit_amount  NUMERIC NOT NULL
);

CREATE INDEX idx_fact_sales_date       ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product    ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer   ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_location   ON fact_sales(location_key);
CREATE INDEX idx_fact_sales_ship_mode  ON fact_sales(ship_mode_key);

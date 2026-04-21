CREATE TABLE summary_monthly_revenue (
    year            INT          NOT NULL,
    month           INT          NOT NULL,
    month_name      VARCHAR(20)  NOT NULL,
    total_revenue   NUMERIC      NOT NULL,
    total_profit    NUMERIC      NOT NULL,
    order_count     INT          NOT NULL,
    PRIMARY KEY (year, month)
);

CREATE TABLE summary_category_performance (
    category_name   VARCHAR(50)  NOT NULL PRIMARY KEY,
    total_revenue   NUMERIC      NOT NULL,
    total_profit    NUMERIC      NOT NULL,
    units_sold      INT          NOT NULL
);

CREATE TABLE summary_region_state_profit (
    region          VARCHAR(20)  NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    total_revenue   NUMERIC      NOT NULL,
    total_profit    NUMERIC      NOT NULL,
    PRIMARY KEY (region, state)
);

CREATE TABLE summary_ship_mode_profitability (
    ship_mode_name  VARCHAR(30)  NOT NULL PRIMARY KEY,
    total_revenue   NUMERIC      NOT NULL,
    total_profit    NUMERIC      NOT NULL,
    profit_ratio    NUMERIC
);

CREATE TABLE summary_customer_ranking (
    customer_key    INT          PRIMARY KEY REFERENCES dim_customer(customer_key),
    customer_name   VARCHAR(100) NOT NULL,
    total_revenue   NUMERIC      NOT NULL,
    order_count     INT          NOT NULL
);

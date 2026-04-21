BEGIN;

TRUNCATE summary_monthly_revenue;
INSERT INTO summary_monthly_revenue
    (year, month, month_name, total_revenue, total_profit, order_count)
SELECT
    d.year, d.month, d.month_name,
    SUM(f.sales_amount)  AS total_revenue,
    SUM(f.profit_amount) AS total_profit,
    COUNT(*)             AS order_count
FROM   fact_sales f
JOIN   dim_date  d ON d.date_key = f.date_key
GROUP  BY d.year, d.month, d.month_name;

TRUNCATE summary_category_performance;
INSERT INTO summary_category_performance
    (category_name, total_revenue, total_profit, units_sold)
SELECT
    p.category_name,
    SUM(f.sales_amount)  AS total_revenue,
    SUM(f.profit_amount) AS total_profit,
    SUM(f.quantity)      AS units_sold
FROM   fact_sales  f
JOIN   dim_product p ON p.product_key = f.product_key
GROUP  BY p.category_name;

TRUNCATE summary_region_state_profit;
INSERT INTO summary_region_state_profit
    (region, state, total_revenue, total_profit)
SELECT
    l.region, l.state,
    SUM(f.sales_amount)  AS total_revenue,
    SUM(f.profit_amount) AS total_profit
FROM   fact_sales   f
JOIN   dim_location l ON l.location_key = f.location_key
GROUP  BY l.region, l.state;

TRUNCATE summary_ship_mode_profitability;
INSERT INTO summary_ship_mode_profitability
    (ship_mode_name, total_revenue, total_profit, profit_ratio)
SELECT
    s.ship_mode_name,
    SUM(f.sales_amount)                                           AS total_revenue,
    SUM(f.profit_amount)                                          AS total_profit,
    SUM(f.profit_amount) / NULLIF(SUM(f.sales_amount), 0)         AS profit_ratio
FROM   fact_sales    f
JOIN   dim_ship_mode s ON s.ship_mode_key = f.ship_mode_key
GROUP  BY s.ship_mode_name;

TRUNCATE summary_customer_ranking;
INSERT INTO summary_customer_ranking
    (customer_key, customer_name, total_revenue, order_count)
SELECT
    c.customer_key, c.customer_name,
    SUM(f.sales_amount) AS total_revenue,
    COUNT(*)            AS order_count
FROM   fact_sales   f
JOIN   dim_customer c ON c.customer_key = f.customer_key
GROUP  BY c.customer_key, c.customer_name;

COMMIT;

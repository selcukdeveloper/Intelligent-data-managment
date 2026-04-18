QUERIES = {
    "top_products_by_revenue": {
        "description": "Top 10 products by total revenue",
        "query": """
MATCH (s:Sale)-[:OF_PRODUCT]->(p:Product)
RETURN p.product_name AS product_name,
       SUM(s.sales_amount) AS revenue
ORDER BY revenue DESC
LIMIT 10;
""".strip(),
    },

    "monthly_revenue_trend": {
        "description": "Monthly revenue trend",
        "query": """
MATCH (s:Sale)-[:ON_DATE]->(:Date)-[:IN_MONTH]->(m:Month)
RETURN m.year AS year,
       m.month_name AS month,
       SUM(s.sales_amount) AS revenue
ORDER BY year, month;
""".strip(),
    },

    "top_customers": {
        "description": "Top 10 customers by spend",
        "query": """
MATCH (s:Sale)-[:BY_CUSTOMER]->(c:Customer)
RETURN c.customer_name AS customer_name,
       SUM(s.sales_amount) AS total_spent
ORDER BY total_spent DESC
LIMIT 10;
""".strip(),
    },

    "revenue_by_category": {
        "description": "Revenue by product category",
        "query": """
MATCH (s:Sale)-[:OF_PRODUCT]->(:Product)-[:IN_CATEGORY]->(c:Category)
RETURN c.category_name AS category,
       SUM(s.sales_amount) AS total_sales
ORDER BY total_sales DESC;
""".strip(),
    },

    "profit_by_region_state": {
        "description": "Profit by region and state",
        "query": """
MATCH (s:Sale)-[:SHIPPED_TO]->(st:State)
RETURN st.state_name AS state,
       SUM(s.profit_amount) AS total_profit
ORDER BY total_profit DESC;
""".strip(),
    },

    "products_high_sales_low_profit": {
        "description": "Products with high sales but low profit",
        "query": """
MATCH (s:Sale)-[:OF_PRODUCT]->(p:Product)
WITH p.product_name AS product_name,
     SUM(s.sales_amount) AS total_sales,
     SUM(s.profit_amount) AS total_profit
RETURN product_name, total_sales, total_profit
ORDER BY total_sales DESC, total_profit ASC
LIMIT 10;
""".strip(),
    },

    "products_low_sales_high_profit": {
        "description": "Products with low sales but high profit",
        "query": """
MATCH (s:Sale)-[:OF_PRODUCT]->(p:Product)
WITH p.product_name AS product_name,
     SUM(s.sales_amount) AS total_sales,
     SUM(s.profit_amount) AS total_profit
RETURN product_name, total_sales, total_profit
ORDER BY total_sales ASC, total_profit DESC
LIMIT 10;
""".strip(),
    },

    "profit_ratio_by_ship_mode": {
        "description": "Shipping mode with highest profit ratio",
        "query": """
MATCH (s:Sale)-[:USES_SHIP_MODE]->(sm:ShipMode)
RETURN sm.ship_mode_name AS ship_mode,
       SUM(s.profit_amount) / nullIf(SUM(s.sales_amount), 0) AS profit_ratio
ORDER BY profit_ratio DESC;
""".strip(),
    },

    "most_frequent_customers": {
        "description": "Customers with highest number of orders",
        "query": """
MATCH (s:Sale)-[:BY_CUSTOMER]->(c:Customer)
RETURN c.customer_name AS customer_name,
       COUNT(s) AS order_count
ORDER BY order_count DESC
LIMIT 10;
""".strip(),
    },

    "lowest_profit_months": {
        "description": "Months with the lowest profit",
        "query": """
MATCH (s:Sale)-[:ON_DATE]->(:Date)-[:IN_MONTH]->(m:Month)
RETURN m.year AS year,
       m.month_name AS month,
       SUM(s.profit_amount) AS total_profit
ORDER BY total_profit ASC
LIMIT 10;
""".strip(),
    },
}

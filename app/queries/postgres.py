QUERIES = {
    "top_products_by_revenue": {
        "description": "Top 10 products by total revenue",
        "query": """
            SELECT
                p.product_name,
                SUM(f.sales_amount) AS revenue
            FROM fact_sales f
            JOIN dim_product p
                ON f.product_key = p.product_key
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 10;
        """.strip(),
    },

    "monthly_revenue_trend": {
        "description": "Monthly revenue trend",
        "query": """
            SELECT
                d.year,
                d.month,
                SUM(f.sales_amount) AS revenue
            FROM fact_sales f
            JOIN dim_date d
                ON f.date_key = d.date_key
            GROUP BY d.year, d.month
            ORDER BY d.year, d.month;
        """.strip(),
    },

    "top_customers": {
        "description": "Top 10 customers by spend",
        "query": """
            SELECT
                c.customer_name,
                SUM(f.sales_amount) AS total_spent,
                COUNT(*) AS order_count
            FROM fact_sales f
            JOIN dim_customer c
                ON f.customer_key = c.customer_key
            GROUP BY c.customer_name
            ORDER BY total_spent DESC
            LIMIT 10;
        """.strip(),
    },

    "revenue_by_category": {
        "description": "Revenue by product category",
        "query": """
            SELECT
                p.category_name AS category,
                SUM(f.sales_amount) AS total_sales
            FROM fact_sales f
            JOIN dim_product p
                ON f.product_key = p.product_key
            GROUP BY p.category_name
            ORDER BY total_sales DESC;
        """.strip(),
    },

    "profit_by_region_state": {
        "description": "Profit by region and state",
        "query": """
            SELECT
                l.region,
                l.state,
                SUM(f.profit_amount) AS total_profit
            FROM fact_sales f
            JOIN dim_location l
                ON f.location_key = l.location_key
            GROUP BY l.region, l.state
            ORDER BY total_profit DESC;
        """.strip(),
    },

    "products_high_sales_low_profit": {
        "description": "Products with high sales but low profit",
        "query": """
            SELECT
                p.product_name,
                SUM(f.sales_amount) AS total_sales,
                SUM(f.profit_amount) AS total_profit
            FROM fact_sales f
            JOIN dim_product p
                ON f.product_key = p.product_key
            GROUP BY p.product_name
            ORDER BY total_sales DESC, total_profit ASC
            LIMIT 10;
        """.strip(),
    },

    "products_low_sales_high_profit": {
        "description": "Products with low sales but high profit",
        "query": """
            SELECT
                p.product_name,
                SUM(f.sales_amount) AS total_sales,
                SUM(f.profit_amount) AS total_profit
            FROM fact_sales f
            JOIN dim_product p
                ON f.product_key = p.product_key
            GROUP BY p.product_name
            ORDER BY total_sales ASC, total_profit DESC
            LIMIT 10;
        """.strip(),
    },

    "profit_ratio_by_ship_mode": {
        "description": "Shipping mode with highest profit ratio",
        "query": """
            SELECT
                s.ship_mode_name AS ship_mode,
                SUM(f.profit_amount) / NULLIF(SUM(f.sales_amount), 0) AS profit_ratio
            FROM fact_sales f
            JOIN dim_ship_mode s
                ON f.ship_mode_key = s.ship_mode_key
            GROUP BY s.ship_mode_name
            ORDER BY profit_ratio DESC;
        """.strip(),
    },

    "most_frequent_customers": {
        "description": "Customers with highest number of orders",
        "query": """
            SELECT
                c.customer_name,
                COUNT(f.sales_fact_id) AS order_count
            FROM fact_sales f
            JOIN dim_customer c
                ON f.customer_key = c.customer_key
            GROUP BY c.customer_name
            ORDER BY order_count DESC
            LIMIT 10;
        """.strip(),
    },

    "lowest_profit_months": {
        "description": "Months with the lowest profit",
        "query": """
            SELECT
                d.year,
                d.month_name AS month,
                SUM(f.profit_amount) AS total_profit
            FROM fact_sales f
            JOIN dim_date d
                ON f.date_key = d.date_key
            GROUP BY d.year, d.month_name
            ORDER BY total_profit ASC;
        """.strip(),
    },
}

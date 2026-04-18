CHART_PRESETS = {
    "top_products_by_revenue": {
        "type": "bar",
        "labelColumns": ["product_name"],
        "valueColumn": "revenue",
    },
    "monthly_revenue_trend": {
        "type": "line",
        "labelColumns": ["year", "month"],
        "labelSeparator": " - ",
        "valueColumn": "revenue",
    },
    "top_customers": {
        "type": "bar",
        "labelColumns": ["customer_name"],
        "valueColumn": "total_spent",
    },
    "revenue_by_category": {
        "type": "bar",
        "labelColumns": ["category"],
        "valueColumn": "total_sales",
    },
    "profit_by_region_state": {
        "type": "bar",
        "labelColumns": ["region", "state"],
        "labelSeparator": " / ",
        "valueColumn": "total_profit",
    },
    "products_high_sales_low_profit": {
        "type": "scatter",
        "labelColumn": "product_name",
        "xColumn": "total_sales",
        "yColumn": "total_profit",
    },
    "products_low_sales_high_profit": {
        "type": "scatter",
        "labelColumn": "product_name",
        "xColumn": "total_sales",
        "yColumn": "total_profit",
    },
    "profit_ratio_by_ship_mode": {
        "type": "bar",
        "labelColumns": ["ship_mode"],
        "valueColumn": "profit_ratio",
    },
    "most_frequent_customers": {
        "type": "bar",
        "labelColumns": ["customer_name"],
        "valueColumn": "order_count",
    },
    "lowest_profit_months": {
        "type": "bar",
        "labelColumns": ["year", "month"],
        "labelSeparator": " - ",
        "valueColumn": "total_profit",
    },
}

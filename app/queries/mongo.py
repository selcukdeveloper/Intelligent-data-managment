QUERIES = {
    "top_products_by_revenue": {
        "description": "Top 10 products by total revenue",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": "$product.product_name",
        "revenue": { "$sum": "$sales_amount" }
      }
    },
    { "$sort": { "revenue": -1 } },
    { "$limit": 10 },
    {
      "$project": {
        "_id": 0,
        "product_name": "$_id",
        "revenue": 1
      }
    }
  ]
}""",
    },

    "monthly_revenue_trend": {
        "description": "Monthly revenue trend",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": {
          "year": "$date.year",
          "month": "$date.month",
          "month_name": "$date.month_name"
        },
        "revenue": { "$sum": "$sales_amount" }
      }
    },
    {
      "$sort": {
        "_id.year": 1,
        "_id.month": 1
      }
    },
    {
      "$project": {
        "_id": 0,
        "year": "$_id.year",
        "month_name": "$_id.month_name",
        "revenue": 1
      }
    }
  ]
}""",
    },

    "top_customers": {
        "description": "Top 10 customers by spend",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": "$customer.customer_name",
        "total_spent": { "$sum": "$sales_amount" }
      }
    },
    { "$sort": { "total_spent": -1 } },
    { "$limit": 10 },
    {
      "$project": {
        "_id": 0,
        "customer_name": "$_id",
        "total_spent": 1
      }
    }
  ]
}""",
    },

    "revenue_by_category": {
        "description": "Revenue by product category",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": "$product.category",
        "total_sales": { "$sum": "$sales_amount" }
      }
    },
    { "$sort": { "total_sales": -1 } },
    {
      "$project": {
        "_id": 0,
        "category": "$_id",
        "total_sales": 1
      }
    }
  ]
}""",
    },

    "profit_by_state": {
        "description": "Profit by state",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": "$location.state",
        "total_profit": { "$sum": "$profit_amount" }
      }
    },
    { "$sort": { "total_profit": -1 } },
    {
      "$project": {
        "_id": 0,
        "state": "$_id",
        "total_profit": 1
      }
    }
  ]
}""",
    },

    "products_high_sales_low_profit": {
        "description": "Products with high sales but low profit",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": "$product.product_name",
        "total_sales": { "$sum": "$sales_amount" },
        "total_profit": { "$sum": "$profit_amount" }
      }
    },
    { "$sort": { "total_sales": -1, "total_profit": 1 } },
    { "$limit": 10 },
    {
      "$project": {
        "_id": 0,
        "product_name": "$_id",
        "total_sales": 1,
        "total_profit": 1
      }
    }
  ]
}""",
    },

    "products_low_sales_high_profit": {
        "description": "Products with low sales but high profit",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": "$product.product_name",
        "total_sales": { "$sum": "$sales_amount" },
        "total_profit": { "$sum": "$profit_amount" }
      }
    },
    { "$sort": { "total_sales": 1, "total_profit": -1 } },
    { "$limit": 10 },
    {
      "$project": {
        "_id": 0,
        "product_name": "$_id",
        "total_sales": 1,
        "total_profit": 1
      }
    }
  ]
}""",
    },

    "profit_ratio_by_ship_mode": {
        "description": "Shipping mode with highest profit ratio",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": {
          "$ifNull": [
            "$ship_mode.ship_mode_name",
            { "$ifNull": ["$ship_mode_name", "$ship_mode"] }
          ]
        },
        "total_profit": { "$sum": "$profit_amount" },
        "total_sales": { "$sum": "$sales_amount" }
      }
    },
    { "$match": { "_id": { "$ne": null } } },
    {
      "$addFields": {
        "profit_ratio": {
          "$cond": [
            { "$eq": ["$total_sales", 0] },
            null,
            { "$divide": ["$total_profit", "$total_sales"] }
          ]
        }
      }
    },
    { "$sort": { "profit_ratio": -1 } },
    {
      "$project": {
        "_id": 0,
        "ship_mode": "$_id",
        "profit_ratio": 1
      }
    }
  ]
}""",
    },

    "most_frequent_customers": {
        "description": "Customers with highest number of orders",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": {
          "$ifNull": [
            "$customer.customer_name",
            { "$ifNull": ["$customer_name", "$customer"] }
          ]
        },
        "order_count": { "$sum": 1 }
      }
    },
    { "$match": { "_id": { "$ne": null } } },
    { "$sort": { "order_count": -1 } },
    { "$limit": 10 },
    {
      "$project": {
        "_id": 0,
        "customer_name": "$_id",
        "order_count": 1
      }
    }
  ]
}""",
    },

    "lowest_profit_months": {
        "description": "Months with the lowest profit",
        "query": """{
  "collection": "sales",
  "operation": "aggregate",
  "pipeline": [
    {
      "$group": {
        "_id": {
          "year": { "$ifNull": ["$date.year", "$year"] },
          "month": {
            "$ifNull": [
              "$date.month_name",
              { "$ifNull": ["$date.month", "$month"] }
            ]
          }
        },
        "total_profit": { "$sum": "$profit_amount" }
      }
    },
    {
      "$match": {
        "_id.year": { "$ne": null },
        "_id.month": { "$ne": null }
      }
    },
    { "$sort": { "total_profit": 1 } },
    { "$limit": 10 },
    {
      "$project": {
        "_id": 0,
        "year": "$_id.year",
        "month": "$_id.month",
        "total_profit": 1
      }
    }
  ]
}""",
    },
}

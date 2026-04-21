MATCH (s:Sale)-[:ON_DATE]->(:Date)-[:IN_MONTH]->(m:Month)
WITH m, sum(s.sales_amount) AS total_revenue,
        sum(s.profit_amount) AS total_profit,
        count(s)             AS order_count
MERGE (agg:MonthlyRevenue {year: m.year, month: m.month})
SET   agg.month_name    = m.month_name,
      agg.total_revenue = total_revenue,
      agg.total_profit  = total_profit,
      agg.order_count   = order_count
MERGE (agg)-[:SUMMARISES]->(m);

MATCH (s:Sale)-[:OF_PRODUCT]->(:Product)-[:IN_CATEGORY]->(c:Category)
WITH c, sum(s.sales_amount)  AS total_revenue,
        sum(s.profit_amount) AS total_profit,
        sum(s.quantity)      AS units_sold
MERGE (agg:CategoryPerformance {category_name: c.category_name})
SET   agg.total_revenue = total_revenue,
      agg.total_profit  = total_profit,
      agg.units_sold    = units_sold
MERGE (agg)-[:SUMMARISES]->(c);

MATCH (s:Sale)-[:SHIPPED_TO]->(st:State)
WITH st, sum(s.sales_amount)  AS total_revenue,
         sum(s.profit_amount) AS total_profit
MERGE (agg:StateProfit {state_name: st.state_name})
SET   agg.region        = st.region,
      agg.total_revenue = total_revenue,
      agg.total_profit  = total_profit
MERGE (agg)-[:SUMMARISES]->(st);

MATCH (s:Sale)-[:USES_SHIP_MODE]->(sm:ShipMode)
WITH sm, sum(s.sales_amount)  AS total_revenue,
         sum(s.profit_amount) AS total_profit
MERGE (agg:ShipModeProfitability {ship_mode_name: sm.ship_mode_name})
SET   agg.total_revenue = total_revenue,
      agg.total_profit  = total_profit,
      agg.profit_ratio  = CASE WHEN total_revenue = 0 THEN null
                               ELSE total_profit / total_revenue END
MERGE (agg)-[:SUMMARISES]->(sm);

MATCH (s:Sale)-[:BY_CUSTOMER]->(c:Customer)
WITH c, sum(s.sales_amount) AS total_revenue, count(s) AS order_count
MERGE (agg:CustomerRanking {customer_name: c.customer_name})
SET   agg.total_revenue = total_revenue,
      agg.order_count   = order_count
MERGE (agg)-[:SUMMARISES]->(c);

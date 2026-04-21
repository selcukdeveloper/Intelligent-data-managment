# MongoDB summary-collection specifications

MongoDB does not have the concept of a pre-aggregated "summary table", but
the equivalent pattern is a **materialised collection fed by an aggregation
pipeline**. The pipeline reads from the main `sales` collection, groups by
the dimension(s) of interest, and writes the result to a second collection
using `$merge` (or `$out`).

A scheduled batch job (cron, Airflow, or a MongoDB Atlas Trigger) re-runs
each pipeline nightly. Reads by the umbrella UI then hit the small summary
collection instead of aggregating the full fact collection at query time.

## Source collection

`sales` — one document per order line, dimensions embedded. See
Methods §MongoDB Document Design for the shape.

## Materialised collections

### `summary_monthly_revenue`

```javascript
db.sales.aggregate([
  { $group: {
      _id: { year: "$date.year", month: "$date.month" },
      month_name:   { $first: "$date.month_name" },
      total_revenue:{ $sum:   "$sales_amount"   },
      total_profit: { $sum:   "$profit_amount"  },
      order_count:  { $sum:   1 }
  }},
  { $project: {
      _id:          0,
      year:         "$_id.year",
      month:        "$_id.month",
      month_name:   1,
      total_revenue: 1,
      total_profit:  1,
      order_count:   1
  }},
  { $merge: {
      into: "summary_monthly_revenue",
      on:   ["year", "month"],
      whenMatched: "replace",
      whenNotMatched: "insert"
  }}
]);
```

### `summary_category_performance`

```javascript
db.sales.aggregate([
  { $group: {
      _id: "$product.category",
      total_revenue: { $sum: "$sales_amount"  },
      total_profit:  { $sum: "$profit_amount" },
      units_sold:    { $sum: "$quantity"      }
  }},
  { $project: { _id: 0, category: "$_id",
      total_revenue: 1, total_profit: 1, units_sold: 1 }},
  { $merge: { into: "summary_category_performance",
      on: "category", whenMatched: "replace", whenNotMatched: "insert" }}
]);
```

### `summary_region_state_profit`

```javascript
db.sales.aggregate([
  { $group: {
      _id: { region: "$location.region", state: "$location.state" },
      total_revenue: { $sum: "$sales_amount"  },
      total_profit:  { $sum: "$profit_amount" }
  }},
  { $project: { _id: 0,
      region: "$_id.region", state: "$_id.state",
      total_revenue: 1, total_profit: 1 }},
  { $merge: { into: "summary_region_state_profit",
      on: ["region", "state"], whenMatched: "replace", whenNotMatched: "insert" }}
]);
```

### `summary_ship_mode_profitability`

```javascript
db.sales.aggregate([
  { $group: {
      _id: "$ship_mode",
      total_revenue: { $sum: "$sales_amount"  },
      total_profit:  { $sum: "$profit_amount" }
  }},
  { $addFields: {
      profit_ratio: { $cond: [
          { $eq: ["$total_revenue", 0] }, null,
          { $divide: ["$total_profit", "$total_revenue"] }
      ]}
  }},
  { $project: { _id: 0, ship_mode: "$_id",
      total_revenue: 1, total_profit: 1, profit_ratio: 1 }},
  { $merge: { into: "summary_ship_mode_profitability",
      on: "ship_mode", whenMatched: "replace", whenNotMatched: "insert" }}
]);
```

### `summary_customer_ranking`

```javascript
db.sales.aggregate([
  { $group: {
      _id: "$customer.customer_name",
      total_revenue: { $sum: "$sales_amount" },
      order_count:   { $sum: 1 }
  }},
  { $project: { _id: 0, customer_name: "$_id",
      total_revenue: 1, order_count: 1 }},
  { $merge: { into: "summary_customer_ranking",
      on: "customer_name", whenMatched: "replace", whenNotMatched: "insert" }}
]);
```

## Nightly batch specification

Each pipeline above is idempotent. A deployment would run them in sequence
once a day; on Atlas, the simplest schedule is a **Scheduled Trigger**
set to `0 2 * * *` (02:00 UTC) that invokes a Realm function containing
the five `aggregate` calls.

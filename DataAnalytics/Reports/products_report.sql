/*
==========================================================
Product Report
==========================================================
Purpose:
	- This report consolidates info about products and key metrics
Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
*/

IF OBJECT_ID ('gold.product_report', 'V') IS NOT NULL
	DROP VIEW gold.product_report;
GO

CREATE VIEW gold.product_report AS
WITH base_query AS (
-- 1) Base Query: Retrieves core columns from fact_sales and dim_products
	SELECT
		f.order_number,
		f.product_key,
		f.customer_key,
		f.sales_amount,
		f.quantity,
		f.price,
		f.order_date,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost,
		p.start_date
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	WHERE f.order_date IS NOT NULL
),

product_aggregations AS (
-- 2) Product aggregations: Summarizes key metrics at the product level
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT customer_key) AS total_customers,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan,
		MAX(order_date) AS last_sale_date
	FROM base_query
	GROUP BY product_key,
			 product_name,
			 category,
			 subcategory,
			 cost
)

-- 3) Final Query: Combines all product results into one output
SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- Average Order Revenue (AOR)
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_revenue,

	-- Average Monthly Revenue
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales / lifespan
	END AS avg_monthly_revenue
FROM product_aggregations;


--SELECT * FROM gold.product_report



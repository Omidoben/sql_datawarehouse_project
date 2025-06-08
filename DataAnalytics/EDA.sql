/*
=========================================
Exploratory Data Analysis
=========================================
1. Dimension Exploration
Purpose:
    - To explore the structure of dimension tables.

2. Date Range Exploration
	- To understand the range of historical data
	- To determine the temporal boundaries of key data points

3. Measures Exploration
	- To calculate aggregated metrics (e.g., totals, averages) for quick insights.
    - To identify overall trends or spot anomalies.

4. Magnitude Analysis
    - To quantify data and group results by specific dimensions.
    - For understanding data distribution across categories.

5. Ranking analysis
*/


--======================================
-- Dimension Exploration
--======================================

-- customers table
SELECT TOP 1000
	customer_key,
	customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    marital_status,
    gender,
    birth_date,
    create_date
FROM gold.dim_customers;


-- products table
SELECT TOP 1000
	product_key,
    product_id,
    product_number,
	product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    cost,
    product_line,
    start_date
FROM gold.dim_products;


-- 1) Retrieve a list of unique countries from which customers originate
SELECT DISTINCT
	country
FROM gold.dim_customers
ORDER BY country;


-- 2) Retrieve a list of unique categories, subcategories, and products
SELECT DISTINCT
	category,
    subcategory,
	product_name
FROM gold.dim_products
ORDER BY 1,2,3



--======================================
-- Date Range Exploration
--======================================
-- Query the facts table - sales

SELECT
	order_number,
    product_key,
    customer_key,
    order_date,
    shipping_date,
    due_date,
    sales_amount,
    quantity,
    price
FROM gold.fact_sales;


-- 3) Determine the first and last order date and the total duration between the orders in months
SELECT
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS order_range_months
FROM gold.fact_sales;


-- 4) Find the youngest and oldest customer based on birth date
SELECT
	MIN(birth_date) AS oldest_birth_date,
	DATEDIFF(year, MIN(birth_date), GETDATE()) AS oldest_age,
	MAX(birth_date) AS youngest_birth_date,
	DATEDIFF(year, MAX(birth_date), GETDATE()) AS youngest_age
FROM gold.dim_customers;


--======================================
-- Measures Exploration
--======================================

-- 5) Find the total sales
SELECT 
	SUM(sales_amount) AS total_sales
FROM gold.fact_sales;

-- 6) Find how many items have been sold
SELECT
	SUM(quantity) AS total_items
FROM gold.fact_sales;

-- 7) Find the average selling price
SELECT 
	AVG(price) AS avg_price 
FROM gold.fact_sales;

-- 8) Find the Total number of Orders
SELECT 
	COUNT(order_number) AS total_orders 
FROM gold.fact_sales;

-- Since one customer can request several products using one order number, we use DISTINCT to get total unique orders
SELECT 
	COUNT(DISTINCT order_number) AS total_orders 
FROM gold.fact_sales;

-- 9) Find the total number of products
SELECT 
	COUNT(product_name) AS total_products
FROM gold.dim_products;

-- 10) Find the total number of customers that have placed an order
SELECT 
	COUNT(DISTINCT customer_key) AS total_customers
FROM gold.fact_sales;


-- 11) Generate a Report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers;


--=============================================================================

-- Magnitude analysis
-- Is comparing the measure values across different categories and dimensions
-- It helps us understand the importance of different categories


--======================================
-- Magnitude Analysis
--======================================

-- Find total customers by countries
SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;


-- Find total customers by gender
SELECT
	gender,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;


-- Find total products by category
SELECT 
	category,
	COUNT(product_name) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC; 


-- What is the average costs in each category?
SELECT
	category,
	AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC;


-- What is the total revenue generated for each category
SELECT 
	p.category,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- What is the total revenue generated by each customer?
SELECT 
	cu.customer_key,
	cu.first_name,
	cu.last_name,
	SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers cu
ON cu.customer_key = fs.customer_key
GROUP BY 
	cu.customer_key,
	cu.first_name,
	cu.last_name
ORDER BY total_revenue DESC;


-- What is the distribution of sold items across countries
SELECT
    c.country,
    SUM(f.quantity) AS total_sold_items
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sold_items DESC;
	

--======================================
-- Ranking analysis
--======================================

-- Which 5 products Generating the Highest Revenue?

-- Using TOP function
SELECT TOP 5
	p.product_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;

-- Using WITH clause
WITH product_revenue AS (
	SELECT 
		p.product_name,
		SUM(f.sales_amount) AS total_revenue,
		RANK() OVER(ORDER BY SUM(f.sales_amount) DESC) AS product_rank
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
	GROUP BY p.product_name
)
SELECT 
	*
FROM product_revenue
WHERE product_rank <=5;


-- Using a subquerry
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;


-- What are the 5 worst-performing products in terms of sales?
SELECT TOP 5
	p.product_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue;


-- Find the top 10 customers who have generated the highest revenue
SELECT TOP 10
	cu.customer_key,
	cu.first_name,
	cu.last_name,
	SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers cu
ON cu.customer_key = fs.customer_key
GROUP BY 
	cu.customer_key,
	cu.first_name,
	cu.last_name
ORDER BY total_revenue DESC;


-- The 3 customers with the fewest orders placed
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders ;


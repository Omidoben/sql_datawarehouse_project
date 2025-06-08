/* 
==============================================================
Advanced Analytics
==============================================================
1. Change Over time analysis
Purpose:
	- To track trends, growth, and changes in key metrics over time
	- For time series analysis and observing seasonality
	- To measure growth or decline over specific periods

2. Cumulative analysis
Purpose:
    - To calculate running totals or moving averages for key metrics.
    - To track performance over time cumulatively.
    - Useful for growth analysis - demonstrates how the business is progressing over time

3. Performance Analysis (Year-over-Year, Month-over-Month)
Purpose:
    - To measure the performance of products, customers, or regions over time.
    - For benchmarking and identifying high-performing entities.
    - To track yearly trends and growth.

4. Data segmentation
Purpose:
	- To group data into meaningful categories for targeted insights.
    - For customer segmentation, product categorization, or regional analysis.

5. Part-to-Whole Analysis
Purpose:
	- Analyze how an individual part is performing compared to the overall, allowing us to understand 
	  which category has the greatest impact on the business
	- To compare performance or metrics across dimensions or time periods.
    - To evaluate differences between categories.
    - Useful for A/B testing or regional comparisons.
*/


-- 1) Change over time
-- Sales performance, number of customers, and total items sold over time
-- Changes over years and months
SELECT
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);




-- 2) Cumulative analysis
-- Running total for sales over the years and moving average for the price
SELECT
	order_year,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_year) AS running_total_sales,
	avg_price,
	AVG(avg_price) OVER(ORDER BY order_year) AS moving_avg_price
FROM (
	SELECT 
		YEAR(order_date) AS order_year,
		SUM(sales_amount) AS total_sales,
		AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE YEAR(order_date) IS NOT NULL
	GROUP BY YEAR(order_date)
) t



-- 3.) Performance analysis
-- Comparison of total sales of the current year with the previous year's sales

SELECT
	order_year,
	current_year_sales,
	LAG(current_year_sales, 1) OVER (ORDER BY order_year) AS previous_year_sales,
	current_year_sales - LAG(current_year_sales, 1) OVER (ORDER BY order_year) AS change_in_tot_sales,
	CASE WHEN current_year_sales - LAG(current_year_sales, 1) OVER (ORDER BY order_year) > 0 THEN 'Increase'
		 WHEN current_year_sales - LAG(current_year_sales, 1) OVER (ORDER BY order_year) < 0 THEN 'Decrease'
		 ELSE 'No Change'
	END AS [descriptive change]
FROM (
	SELECT 
		YEAR(order_date) AS order_year,
		SUM(sales_amount) AS current_year_sales,
		AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE YEAR(order_date) IS NOT NULL
	GROUP BY YEAR(order_date)
) t



/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */

WITH yearly_product_sales AS (
	SELECT
		p.product_name,
		YEAR(f.order_date) AS order_year,
		SUM(f.sales_amount) AS current_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	WHERE YEAR(f.order_date) IS NOT NULL
	GROUP BY 
		p.product_name,
		YEAR(f.order_date)
	)
SELECT
	product_name,
	order_year,
	current_sales,
	-- current - average comparison
	AVG(current_sales) OVER(PARTITION BY product_name) AS avg_sales,
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS diff_avg,
	CASE WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Average'
		 WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Average'
		 ELSE 'Average'
	END AS current_average_diff,
	-- year over year analysis
	LAG(current_sales, 1) OVER(PARTITION BY product_name ORDER BY order_year) previous_sales,
	current_sales - LAG(current_sales, 1) OVER(PARTITION BY product_name ORDER BY order_year) AS change_in_sales,
	CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
         WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
         ELSE 'No Change'
	END AS descriptive_change
FROM yearly_product_sales
ORDER BY product_name, order_year;



-- 4.) Data segmentation

-- Segment products into cost ranges and count how many products fall into each segment

SELECT 
	cost,
	NTILE(3) OVER(ORDER BY cost) AS cost_bins
FROM gold.dim_products


WITH product_segmentation AS (
	SELECT
		product_key,
		product_name,
		cost,
		CASE WHEN cost < 100 THEN 'Below 100'
			 WHEN cost BETWEEN 100 AND 500 THEN '100 - 500'
			 WHEN cost BETWEEN 500 AND 1000 THEN '500 - 1000'
			 ELSE 'Above 1000'
		END AS cost_range
	FROM gold.dim_products
)
SELECT
	cost_range,
	COUNT(*) AS products_per_segment
FROM product_segmentation
GROUP BY cost_range
ORDER BY products_per_segment DESC;



/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/


WITH customer_spending AS (
	SELECT 
		c.customer_key,
		MIN(f.order_date) AS first_order,
		MAX(f.order_date) AS last_order,
		DATEDIFF(month, MIN(f.order_date), MAX(f.order_date)) AS customer_history,
		SUM(f.sales_amount) AS total_spending
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON f.customer_key = c.customer_key
	GROUP BY c.customer_key
	),
customer_segmentation AS (
	SELECT 
		customer_key,
		CASE WHEN customer_history >= 12 AND total_spending > 5000 THEN 'VIP'
			 WHEN customer_history >= 12 AND total_spending <= 5000 THEN 'Regular'
			 ELSE 'New'
		END AS customer_segments
	FROM customer_spending
)
SELECT
	customer_segments,
	COUNT(*) AS num_customers
FROM customer_segmentation
GROUP BY customer_segments
ORDER BY customer_segments;




-- 5) Part-to-whole Analysis
-- Which categories contribute the most to the overall sales
WITH category_sales AS (
	SELECT 
		p.category,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	GROUP BY p.category
)
SELECT
	category,
	total_sales,
	SUM(total_sales) OVER() AS overall_sales,
	CONCAT(ROUND(CAST(total_sales AS FLOAT) / SUM(total_sales) OVER() * 100, 2), '%') AS percentage_of_total 
FROM category_sales
ORDER BY percentage_of_total 
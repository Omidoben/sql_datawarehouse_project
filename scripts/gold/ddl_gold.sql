-- In this layer, we start by joining all the tables holding customer information: cust_info, cust_az12, loc_a101
-- Check whether duplicates have been introduced after joining the tables

SELECT 
	cst_id,
	COUNT(*) cnt
FROM (
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid) t
GROUP BY cst_id
HAVING COUNT(*) > 1


-- Do data integration on the two gender columns

SELECT 
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the master for gender info
		 ELSE COALESCE(ca.gen, 'n/a')
	END new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
WHERE ci.cst_gndr != ca.gen
ORDER BY 1, 2

-- There are cases where there are mismatches between the two columns

-- Updated code - Rename columns to more descriptive names
-- create a view that holds results of this query

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,		-- Surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the master for gender info
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid


SELECT * FROM gold.dim_customers;
SELECT distinct gender FROM gold.dim_customers;


--===========================================================================================================================

-- products table
SELECT * FROM silver.crm_prd_info;

SELECT * FROM silver.erp_px_cat_g1v2;

-- For this table, we only want to select products that are currently open, not historical data
-- If end date is NULL, then it is current info of the product

-- Also check if duplicates have been introduced on the product_key column

SELECT
	prd_key, 
	COUNT(*) cnt
FROM(
SELECT 
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL) t	-- Filter out historical data
GROUP BY prd_key
HAVING COUNT(*) > 1


-- Updated code
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;


SELECT * FROM gold.dim_products;



--==========================================================================================================================
-- Sales_details table
SELECT * FROM silver.crm_sales_details;

-- We use the dimension's surrogate keys instead of IDs to easily connect facts with dimensions

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales  AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


SELECT * FROM gold.fact_sales

-- Check if the tables can be connected using the customer keys and product keys
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
-- customer info

-- Check for nulls or duplicates in primary key
-- Expectation: No result

SELECT *
FROM bronze.crm_cust_info;


SELECT 
	cst_id,
	COUNT(*) cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- The data has duplicates, and 3 null values

-- Observing the duplicates
SELECT
	*
FROM bronze.crm_cust_info
WHERE cst_id IN (29449 , 29473, 29433, 29483, 29466);

-- Looking at the creation_date, the latest record appears to hold the latest information

-- Handling the duplicates
SELECT 
	*
FROM(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) rnk
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) t
WHERE rnk = 1


-- Check for unwanted spaces
-- Expectation: No results
SELECT *
FROM bronze.crm_cust_info
WHERE LEN(cst_fristname) != LEN(TRIM(cst_fristname))

SELECT *
FROM bronze.crm_cust_info
WHERE LEN(cst_lastname) != LEN(TRIM(cst_lastname))


-- Data Standardization & Consistency

SELECT DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;

SELECT DISTINCT(cst_marital_status)
FROM bronze.crm_cust_info;

-- We don't want abbreviated names in the project, thus we use CASE WHEN statements to transform gender and marital status columns


-- Extend the above code that handles duplicates to handle the unwanted spaces, and also data standardization
-- Insert data into the silver.crm_cust_info table

INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_fristname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status,			-- Normalize marital status to readable format
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 ELSE 'n/a'
	END cst_gndr,		-- Normalize gender to readable format
	cst_create_date
FROM(
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) rnk
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) t
WHERE rnk = 1;		-- Remove duplicates from the customer id key




-- =============================================================================================================

-- Product info

SELECT TOP 1000
	*
FROM bronze.crm_prd_info;


-- Check for duplicates or NULL values
-- Expectation: No results

SELECT 
	prd_id,
	COUNT(*) cnt
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- No duplicates or Nulls in the product id column

-- The product key column can be can be split into two columns: category_id and product_key
-- This will enable easy joining of the products and the erp_px_cat_g1v2 table using the category_id

-- On the other hand, the product key will be used when joining with the sales_details table

SELECT * FROM bronze.erp_px_cat_g1v2;

-- Notably, the id in the erp_px_cat_g1v2 table uses '_' while the product_info uses '-'

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, 
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN		-- Cross check if the rows match
	(SELECT ID FROM bronze.erp_px_cat_g1v2);

-- One product category 'CO_PE' doesn't have a match in the erp_px_cat_g1v2 table


-- Check for unwanted spaces
-- Expectation: No results

SELECT *
FROM bronze.crm_prd_info
WHERE LEN(prd_nm) != LEN(TRIM(prd_nm))

-- There no spaces in the product name


-- Check for NULLS or negative numbers in the product cost
-- Expectation: No results

SELECT
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

-- There are no negatives, but there are two nulls


-- Data Standardization & Consistency

SELECT DISTINCT(prd_line)
FROM bronze.crm_prd_info;


-- Check for invalid date orders
SELECT
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- Investigating a few examples
SELECT TOP 20
	*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt
ORDER BY prd_key

-- Soln 1: Switching start and end dates
-- Issue: The dates are overlapping. Example the AC-HE-HL-U509 product


-- Soln 2
-- End date = Start date of the next record - 1
-- We minus 1 to avoid overlapping dates
-- Convert to DATE format since the time has no values

SELECT
	*,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509', 'AC-HE-HL-U509-B', 'AC-HE-HL-U509-R')



-- Data transformation code

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, 
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info


-- Since we've added a new column, cat_id and modified the data types of start and end columns, we go back to the DDL script to incorporate these changes

-- Then insert the data into the silver.prd_info table

INSERT INTO silver.crm_prd_info (
	prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt )
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,   -- Extract category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,		-- Extract product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,		-- Map product line values to descriptive values
	CAST(prd_start_dt AS DATE) prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1
		AS DATE) 
	AS prd_end_dt		-- Calculate end date as one day before the next start date
FROM bronze.crm_prd_info


SELECT * FROM silver.crm_prd_info;




--================================================================================================================================

-- sales_details

SELECT *
FROM bronze.crm_sales_details;


-- In this table we don't remove the duplicates in the sls_ord_num since one customer can make purchase several orders using the same order number

SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY sls_ord_num ORDER BY sls_price) rnk
FROM bronze.crm_sales_details


-- Check for unwanted spaces
SELECT
	*
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- It doesn't have unwanted spaces


-- Explore the sls_prd_key and sls_cust_id to check if they match with keys in the products and customer tables

SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN 
	(SELECT cst_id FROM silver.crm_cust_info)
--(SELECT prd_key FROM silver.crm_prd_info)

-- Both columns have matching keys


-- Exploring the sls_order_dt, sls_ship_dt, and sls_due_dt

-- Check for invalid dates: where order_dt = 0 or has a length < 8
SELECT 
	NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

-- The sls order date column has zeros, we can convert them to null using the NULLIF()
-- Some two values have length < 8

-- Check for invalid date orders
SELECT 
	*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt


-- Exploring the last 3 columns: sls_sales, sls_quantity, sls_price
-- Check data consistency between sales, quantity, and price
-- Business rule: Sales = Quantity * price
--				  Negatives, nulls, zeros are not allowed

SELECT
	sls_sales old_sales,
	sls_quantity,
	sls_price old_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales

-- There are negatives, zeros, and nulls in the sales and price columns. Also some calculations of Sales = Q * P do not match

-- For such a scenario, we consult experts

-- Suppose we have the following rules:
--	- If sales is negative, zero, or null, derive it using Quantity * price
--	- If price is zero or null, calculate it using sales / quantity
--  - If price is negative, convert it to a positive




-- Transformation code

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END sls_order_dt,
	CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END sls_ship_dt,													-- cast order_dt, ship_dt, due_dt to DATE data type
	CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,		-- Recalculate sales if original value is missing or incorrect
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price				-- Recalculate price if original value is invalid
	END AS sls_price
FROM bronze.crm_sales_details;
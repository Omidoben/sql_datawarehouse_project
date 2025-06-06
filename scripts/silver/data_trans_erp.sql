-- cust_az12

SELECT 
	*
FROM bronze.erp_cust_az12;

SELECT *
FROM silver.crm_cust_info;

-- Since we want to join this cust_az12 table with the cust_info table, we ensure the id columns match in both tables
-- In the cust_az12 table, some rows are prefixed with 'NAS' which is not present in the cust_info table, we thus remove these characters

SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END
NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)   -- Checks if the transformation is working perfectly


-- Identify out of range dates
SELECT
	bdate,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- There are some invalid dates
-- Can choose to report to the experts or convert the dates that are > current date to NULLS


-- Data standardization & consistency
SELECT
	DISTINCT(gen),
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN  'Male'
		 ELSE 'n/a'
	END gen
FROM bronze.erp_cust_az12

-- There are nulls, empty spaces, F, M, Male, Female



-- Transformation code
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,			-- Remove 'NAS' prefix if present
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END bdate,			-- Set future birthdates to NULLS
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN  'Male'
		 ELSE 'n/a'
	END gen    -- Normalize gender values and handle missing values
FROM bronze.erp_cust_az12;



-- =====================================================================================================================================

-- erp_loc_a101

SELECT *
FROM bronze.erp_loc_a101;


SELECT *
FROM silver.crm_cust_info;

-- This table can be joined with the customer info table using customer id column
-- The erp_loc_a101 table has some incosistencies in the cid column, it has '-' which are not present in the cst_key in customer info table

SELECT
	cid,
	REPLACE(cid, '-', '') cid
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)    -- Checks if the rows are matching after transformation


-- Data standardization & consistency
SELECT 
	DISTINCT(cntry),
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END cntry
FROM bronze.erp_loc_a101
-- There are nulls, empty strings, and some other incosistencies



-- Transformation code

INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry		 -- Normalize and handle missing or blank country values
FROM bronze.erp_loc_a101;


-- ====================================================================================================================================

SELECT *
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.crm_prd_info;

-- The id column in the px_cat_g1v2 can be joined with the cat_id in the products table

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)   -- The rows match


-- Check for unwanted spaces
SELECT
	*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)   

-- There no unwanted spaces in the cat. subcat, and maintenance columns


-- Data standardization & consistency
SELECT 
	DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT 
	DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

-- All columns are perfect


-- Insert the data into the silver table
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2


SELECT * FROM silver.erp_px_cat_g1v2







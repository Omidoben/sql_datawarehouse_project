--======================================
-- Data Analytics Project
--======================================

-- The project starts by database exploration to understand its structure, the available tables and their metadata

/*
========================================
Database Exploration
========================================

Purpose:
	 - To explore the structure, of the database, including the list of available tables and their schemas.
	 - To inspect the columns and metadata for specific tables

Tables Used:
	 - INFORMATION_SCHEMA.TABLES
	 - INFORMATION_SCHEMA.COLUMNS
*/


-- Retrieve a list of all tables in the database
SELECT
	TABLE_CATALOG,
	TABLE_SCHEMA,
	TABLE_NAME,
	TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES;


-- Retrieve all columns for a specific table - consider the products dimension
SELECT 
	COLUMN_NAME,
	DATA_TYPE,
	IS_NULLABLE,
	CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_products';
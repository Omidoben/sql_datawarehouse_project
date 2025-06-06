/* 
=========================================================================
Create Database and Schemas
=========================================================================
Script Purpose:
	This script creates a new database named 'DataWareHouse' after checking if it already exists. If the database already exists, 
	it is dropped and recreated. Additionally, the script sets up three schemas within the database: bronze, silver, and gold.

WARNING: 
	Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


USE master;
GO

-- Drop and recreate the DataWareHouse database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO

-- Create the DataWareHouse database
CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO


-- Start by creating schemas

-- For this project, we're using the Medallion architecture, which has three layers: Bronze, Silver, and Gold
-- We thus create three schemas, one for each layer

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
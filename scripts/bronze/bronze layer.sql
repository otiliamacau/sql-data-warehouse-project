-- CREATE DATABASE
-- CREATE BRONZE, SILVER & GOLD SCHEMAS
-- CREATE BRONZE LAYER

USE master;
GO

IF EXISTS(SELECT 1 from sys.databases WHERE name= 'DataWarehouse') 
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse     
END;
GO

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
 
CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NCHAR(50),
	cst_create_date DATE
);


IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);


IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
 

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
 CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);


IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
 CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);


IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
 CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================================';
	
		PRINT '------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating table: bronze.crm_cust_info';

		TRUNCATE TABLE bronze.crm_cust_info; 

		PRINT '>> Inserting data into: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'F:\Din E\CV\SQL project\full project\crm_source\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; 
		PRINT '>> Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\Din E\CV\SQL project\full project\crm_source\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\Din E\CV\SQL project\full project\crm_source\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------';

		PRINT '------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\Din E\CV\SQL project\full project\erp_source\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'F:\Din E\CV\SQL project\full project\erp_source\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\Din E\CV\SQL project\full project\erp_source\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '======================================';
		PRINT 'Loading Bronze Layer is COMPLETED';
		PRINT '- Total Load Duration:' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '======================================';
	END TRY
	BEGIN CATCH
		PRINT '======================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_Number() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_State() AS NVARCHAR);
		PRINT '======================================';
	END CATCH
END

EXEC bronze.load_bronze;
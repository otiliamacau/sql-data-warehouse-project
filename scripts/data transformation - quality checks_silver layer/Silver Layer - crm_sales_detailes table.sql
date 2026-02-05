SELECT TOP 100 
	*
FROM bronze.crm_sales_details;


-- Check for UNWANTED SPACES
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key)


-- Check for KEY INTEGRITY
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) 

SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


-- Check for INVALID DATE
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) !=8 OR sls_order_dt > 20205040  OR sls_order_dt < 19000021 

SELECT
	NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

SELECT
	sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) !=8 OR sls_ship_dt > 20205040  OR sls_ship_dt < 19000021

SELECT
	sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) !=8 OR sls_due_dt > 20205040  OR sls_due_dt < 19000021


-- Check for INVALID DATE ORDERS
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


--- Check Data Consistency:
--- 1. sales =  quantity * price
--- 2. values must not be NULL, ZERO OR NEGATIVE
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR  sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;


SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales ! = sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales,
	sls_quantity,
	CASE WHEN sls_price < 0 THEN ABS(sls_price)
		WHEN sls_price < = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END sls_price
FROM
(
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY sls_ord_num ORDER BY sls_order_dt DESC) AS rank_sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NOT NULL
)t
WHERE rank_sls_ord_num = 1;


-- INSERT THE VALUES INTO silver.crm_sales_details table
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
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL  -- Handle INVALID DATE
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)            -- Datatype casting
	END sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales ! = sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales,        -------- Recalculate sales if original value is NULL or NEGATIVE or INCORRECT
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price < = 0 THEN sls_sales / NULLIF(sls_quantity, 0) -- Take care if quantity is 0 (can't be divided to 0, so it'll be transformed to NULL)
		ELSE sls_price
	END sls_price		  -------- Recalculate price if original value is NEGATIVE or 0
FROM bronze.crm_sales_details

SELECT * FROM silver.crm_sales_details;
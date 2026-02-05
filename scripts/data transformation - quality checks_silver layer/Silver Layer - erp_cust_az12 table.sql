SELECT
*
FROM bronze.erp_cust_az12;

SELECT * FROM silver.crm_cust_info

-- Check for UNWANTED SPACE
SELECT cid
FROM bronze.erp_cust_az12
WHERE cid != TRIM(cid);

-- Check for key matching between tables
SELECT
	cid,
	CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
		ELSE cid
	END cid,
	bdate,
	gen
FROM bronze.erp_cust_az12;


-- Check for OUT-OF-RANGE DATE
SELECT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();  -- check for old customers (over 100 years)

SELECT
	CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
		ELSE cid
	END cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END bdate,
	gen
FROM bronze.erp_cust_az12;


-- Check for OUT-OF-RANGE DATE
SELECT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();  -- check for old customers (over 100 years)


-- Check for problems with 'gen' column
SELECT DISTINCT gen
FROM bronze.erp_cust_az12


SELECT
	CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
		ELSE cid
	END cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n\a'
	END gen
FROM bronze.erp_cust_az12;


---- INSERT VALUES TO silver.erp_cust_az12 table
INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT
	CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
		ELSE cid
	END cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n\a'
	END gen
FROM bronze.erp_cust_az12;
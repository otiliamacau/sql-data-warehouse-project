SELECT *
FROM bronze.erp_loc_a101;

SELECT *
FROM silver.crm_cust_info;

SELECT
	REPLACE(cid, '-', '') cid,
	cntry
FROM bronze.erp_loc_a101;

-- Check DATA CONSISTENCY
SELECT DISTINCT	
	cntry
FROM bronze.erp_loc_a101;


SELECT
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END cntry
FROM bronze.erp_loc_a101;


---- INSERT VALUES TO silver.erp_loc_a101 table
INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END cntry
FROM bronze.erp_loc_a101;
SELECT *
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.crm_prd_info

--Check for UNWANTED SPACES
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat!= TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

--Data Standardization & Consistency
SELECT DISTINCT
	cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
	subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2;

---- INSERT VALUES TO silver.erp_px_cat_g1v2 table
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
FROM bronze.erp_px_cat_g1v2;
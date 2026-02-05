SELECT
	prd_id,
	count(prd_id)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(prd_id) > 1 OR prd_id IS NULL;

SELECT
	prd_key,
	count(prd_key)
FROM bronze.crm_prd_info
GROUP BY prd_key
HAVING count(prd_key) > 1 OR prd_key IS NULL;


SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

--CHECK FOR UNWANTED SPACES
SELECT
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--CHECK FOR NULL or NEGATIVE NUMBERS
SELECT
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost != ABS(prd_cost) OR prd_cost IS NULL


SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost, --SOLUTION FOR REPLACING NULLs in NUMERIC COLUMN
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


--CHECK FOR INVALID DATE
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--SOLUTION
 SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost, --SOLUTION FOR REPLACING NULLs in NUMERIC COLUMN
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt  ---SOLUTION FOR INVALID DATE
FROM bronze.crm_prd_info

-- INSERT INTO NEW LAYER - SILVER LAYER - table: crm_prd_info
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
 SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost, --SOLUTION FOR REPLACING NULLs in NUMERIC COLUMN
	CASE UPPER(TRIM(prd_line))   
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'   -- Handle the missing value for string column
	END AS prd_line,  -- MAP product line code to descriptive values (Normalization)
	prd_start_dt,
	DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt  ---SOLUTION FOR INVALID DATE (Calculate end date as one day befoare the next start date)
FROM bronze.crm_prd_info;

SELECT * FROM silver.crm_prd_info;
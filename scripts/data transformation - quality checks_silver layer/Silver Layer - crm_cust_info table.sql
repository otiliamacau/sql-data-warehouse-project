-------- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
SELECT 
	cst_id,
	count(*) number_of_ids
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

-- SOLUTION
SELECT
*
FROM
(
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) id_rank
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t
WHERE id_rank = 1;


-------- CHECK FOR UNWANTED SPACES IN STRING VALUES
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- SOLUTION
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status,		--- Normalize marital status to readable format
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,		--- Normalize marital status to readable format
	cst_create_date
FROM 
(
SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) id_rank
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t
WHERE id_rank = 1;

-- INSERT INTO NEW LAYER - SILVER LAYER - table:crm_cust_info
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM 
(
SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) id_rank
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t
WHERE id_rank = 1;

select * from silver.crm_cust_info;

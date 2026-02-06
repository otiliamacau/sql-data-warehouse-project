/*
===================PRODUCT REPORT=================
*/

CREATE VIEW gold.products_report AS
WITH base_product_query AS
(
SELECT
	f.order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
)
,product_aggregations AS
(
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	COUNT(DISTINCT order_number) as total_orders,
	SUM(sales_amount) as total_sales,
	SUM(quantity) AS total_quantity_sold,
	COUNT(DISTINCT customer_key) as total_customers,
	MAX(order_date) as last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) as lifespan
FROM base_product_query
GROUP BY
	product_key,
	product_name,
	category,
	subcategory,
	cost
)
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_order_date,
	CASE WHEN total_sales > 50000 THEN 'High-Performer'
		 WHEN total_sales <= 50000 THEN 'Mid-Range'
		 ELSE 'Low-Performer'
	END as product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity_sold,
	total_customers,
	DATEDIFF(month, last_order_date, GETDATE()) as recency,
	CASE WHEN total_orders = 0 THEN 0
		 ELSE total_sales / total_orders
	END as avg_order_revenue,
	CASE WHEN lifespan = 0 THEN total_sales
		 ELSE total_sales/lifespan
	END as avg_monthly_revenue
FROM product_aggregations;

SELECT * FROM gold.products_report;


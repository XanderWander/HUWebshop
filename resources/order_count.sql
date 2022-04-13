DROP TABLE IF EXISTS order_count;
CREATE TABLE order_count AS (
	SELECT
		pid, ct,
		categories.name AS category,
		sub_categories.name AS sub_category,
		sub_sub_categories.name AS sub_sub_category
	FROM (
		SELECT product_id as pid, COUNT(product_id) AS ct
		FROM ordered_products
		GROUP BY 1
	) AS x
	JOIN products ON products.id = pid
	JOIN categories ON categories.id = products.category_id
	JOIN sub_categories ON sub_categories.id = products.sub_category_id
	JOIN sub_sub_categories ON sub_sub_categories.id = products.sub_sub_category_id
	ORDER BY ct desc
);
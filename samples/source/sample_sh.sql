-- Extracted from: sample_sh.sh
-- Total statements: 5
--==================================================

-- Statement 1: CREATE
CREATE VOLATILE TABLE TEMP_SALES_DATA AS (
SELECT DISTINCT
p.product_id,
p.product_name,
p.category,
s.sale_id,
s.sale_date,
s.quantity,
s.unit_price,
s.total_amount
FROM PRODUCT_DIM p
LEFT OUTER JOIN SALES_FACT s
ON p.product_id = s.product_id
WHERE s.sale_date >= CURRENT_DATE - 60
AND p.product_status = 'ACTIVE'
) WITH DATA ON COMMIT PRESERVE ROWS;

-- Statement 2: UPDATE
UPDATE A FROM PRODUCT_SUMMARY A,
(SELECT product_id, COUNT(*) as sale_count, SUM(total_amount) as total_revenue
FROM TEMP_SALES_DATA
GROUP BY product_id) B
SET sale_count = B.sale_count,
total_revenue = B.total_revenue,
last_updated = CURRENT_TIMESTAMP
WHERE A.product_id = B.product_id;

-- Statement 3: INSERT
INSERT INTO PRODUCT_DETAILS
(product_id, product_name, category, sale_count, total_revenue,
created_date, last_updated)
SELECT
product_id,
product_name,
category,
COUNT(*) as sale_count,
SUM(total_amount) as total_revenue,
CURRENT_DATE as created_date,
CURRENT_TIMESTAMP as last_updated
FROM TEMP_SALES_DATA
GROUP BY product_id, product_name, category;

-- Statement 4: INSERT
INSERT INTO SALES_REPORTING
(report_date, product_id, product_name, category_name,
sale_count, total_revenue, region, sales_channel)
SELECT
CURRENT_DATE as report_date,
p.product_id,
p.product_name,
c.category_name,
COUNT(s.sale_id) as sale_count,
SUM(s.total_amount) as total_revenue,
r.region_name as region,
sc.channel_name as sales_channel
FROM TEMP_SALES_DATA p
LEFT OUTER JOIN CATEGORY_REF c
ON p.category = c.category_code
LEFT OUTER JOIN SALES_FACT s
ON p.product_id = s.product_id
LEFT OUTER JOIN REGION_DIM r
ON s.region_id = r.region_id
LEFT OUTER JOIN SALES_CHANNEL_DIM sc
ON s.channel_id = sc.channel_id
WHERE s.sale_date >= CURRENT_DATE - 90
GROUP BY p.product_id, p.product_name, c.category_name, r.region_name, sc.channel_name;

-- Statement 5: INSERT
INSERT INTO ETL_AUDIT_LOG
(process_name, table_name, record_count, process_date, status)
SELECT
'SALES_ETL' as process_name,
'SALES_REPORTING' as table_name,
COUNT(*) as record_count,
CURRENT_DATE as process_date,
'COMPLETED' as status
FROM SALES_REPORTING
WHERE report_date = CURRENT_DATE;


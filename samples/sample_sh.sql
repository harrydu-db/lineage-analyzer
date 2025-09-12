-- Extracted SQL statements from shell script
-- Total statements: 5
-- ============================================================

-- Statement 1 (Original line: 1)
CREATE VOLATILE TABLE TEMP_SALES_DATA AS
    (SELECT DISTINCT p.product_id,
                     p.product_name,
                     p.category,
                     s.sale_id,
                     s.sale_date,
                     s.quantity,
                     s.unit_price,
                     s.total_amount
     FROM PRODUCT_DIM p
     LEFT OUTER JOIN SALES_FACT s ON p.product_id = s.product_id
     WHERE s.sale_date >= CURRENT_DATE - 60
         AND p.product_status = 'ACTIVE') WITH DATA ON
COMMIT PRESERVE ROWS;

-- Statement 2 (Original line: 18)
UPDATE A
FROM PRODUCT_SUMMARY A,

    (SELECT product_id,
            COUNT(*) AS sale_count,
            SUM(total_amount) AS total_revenue
     FROM TEMP_SALES_DATA
     GROUP BY product_id) B
SET sale_count = B.sale_count,
    total_revenue = B.total_revenue,
    last_updated = CURRENT_TIMESTAMP
WHERE A.product_id = B.product_id;

-- Statement 3 (Original line: 27)
INSERT INTO PRODUCT_DETAILS (product_id, product_name, category, sale_count, total_revenue, created_date, last_updated)
SELECT product_id,
       product_name,
       category,
       COUNT(*) AS sale_count,
       SUM(total_amount) AS total_revenue,
       CURRENT_DATE AS created_date,
                       CURRENT_TIMESTAMP AS last_updated
FROM TEMP_SALES_DATA
GROUP BY product_id,
         product_name,
         category;

-- Statement 4 (Original line: 41)
INSERT INTO SALES_REPORTING (report_date, product_id, product_name, category_name, sale_count, total_revenue, region, sales_channel)
SELECT CURRENT_DATE AS report_date,
                       p.product_id,
                       p.product_name,
                       c.category_name,
                       COUNT(s.sale_id) AS sale_count,
                       SUM(s.total_amount) AS total_revenue,
                       r.region_name AS region,
                       sc.channel_name AS sales_channel
FROM TEMP_SALES_DATA p
LEFT OUTER JOIN CATEGORY_REF c ON p.category = c.category_code
LEFT OUTER JOIN SALES_FACT s ON p.product_id = s.product_id
LEFT OUTER JOIN REGION_DIM r ON s.region_id = r.region_id
LEFT OUTER JOIN SALES_CHANNEL_DIM sc ON s.channel_id = sc.channel_id
WHERE s.sale_date >= CURRENT_DATE - 90
GROUP BY p.product_id,
         p.product_name,
         c.category_name,
         r.region_name,
         sc.channel_name;

-- Statement 5 (Original line: 65)
INSERT INTO ETL_AUDIT_LOG (process_name, TABLE_NAME, record_count, process_date, status)
SELECT 'SALES_ETL' AS process_name,
       'SALES_REPORTING' AS TABLE_NAME,
       COUNT(*) AS record_count,
       CURRENT_DATE AS process_date,
                       'COMPLETED' AS status
FROM SALES_REPORTING
WHERE report_date = CURRENT_DATE;


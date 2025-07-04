-- Sample ETL SQL file for testing lineage analysis
-- This file demonstrates various SQL operations commonly found in ETL processes

-- Create a temporary table for data processing
CREATE VOLATILE TABLE TEMP_CUSTOMER_DATA AS (
SELECT DISTINCT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    o.order_id,
    o.order_date,
    o.total_amount
FROM CUSTOMER_DIM c
LEFT OUTER JOIN ORDER_FACT o
ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - 30
AND c.customer_status = 'ACTIVE'
) WITH DATA ON COMMIT PRESERVE ROWS;

-- Update customer summary table
UPDATE A FROM CUSTOMER_SUMMARY A, 
(SELECT customer_id, COUNT(*) as order_count, SUM(total_amount) as total_spent 
 FROM TEMP_CUSTOMER_DATA 
 GROUP BY customer_id) B
SET order_count = B.order_count,
    total_spent = B.total_spent,
    last_updated = CURRENT_TIMESTAMP
WHERE A.customer_id = B.customer_id;

-- Insert new customer records
INSERT INTO CUSTOMER_DETAILS
(customer_id, customer_name, customer_type, order_count, total_spent, 
 created_date, last_updated)
SELECT 
    customer_id,
    customer_name,
    customer_type,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent,
    CURRENT_DATE as created_date,
    CURRENT_TIMESTAMP as last_updated
FROM TEMP_CUSTOMER_DATA
GROUP BY customer_id, customer_name, customer_type;

-- Insert into reporting table with reference data
INSERT INTO CUSTOMER_REPORTING
(report_date, customer_id, customer_name, customer_segment, 
 order_count, total_spent, region, sales_rep)
SELECT
    CURRENT_DATE as report_date,
    c.customer_id,
    c.customer_name,
    s.segment_name as customer_segment,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_spent,
    r.region_name as region,
    sr.rep_name as sales_rep
FROM TEMP_CUSTOMER_DATA c
LEFT OUTER JOIN SEGMENT_REF s
ON c.customer_type = s.customer_type
LEFT OUTER JOIN ORDER_FACT o
ON c.customer_id = o.customer_id
LEFT OUTER JOIN REGION_DIM r
ON o.region_id = r.region_id
LEFT OUTER JOIN SALES_REP_DIM sr
ON o.rep_id = sr.rep_id
WHERE o.order_date >= CURRENT_DATE - 90
GROUP BY c.customer_id, c.customer_name, s.segment_name, r.region_name, sr.rep_name;

-- Insert into audit log
INSERT INTO ETL_AUDIT_LOG
(process_name, table_name, record_count, process_date, status)
SELECT
    'CUSTOMER_ETL' as process_name,
    'CUSTOMER_REPORTING' as table_name,
    COUNT(*) as record_count,
    CURRENT_DATE as process_date,
    'COMPLETED' as status
FROM CUSTOMER_REPORTING
WHERE report_date = CURRENT_DATE; 
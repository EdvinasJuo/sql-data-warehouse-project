/*
===================================================================================
Stored Procedure: Load Silver Layer (Bronze --> Silver)
===================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
  Actions Performed:
    - Truncates Silver Tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None.

Usage Example:
  CALL silver.load_silver();
===================================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark

def main(session):
    try:
        # 1. Truncate silver.crm_cust_info
        session.sql("TRUNCATE TABLE silver.crm_cust_info").collect()
        
        # 1.1 Insert into silver.crm_cust_info
        session.sql("""
            INSERT INTO silver.crm_cust_info (
                cst_id, cst_key, cst_firstname, cst_lastname, 
                cst_marital_status, cst_gndr, cst_create_date
            )
            SELECT 
                cst_id, cst_key, TRIM(cst_firstname), TRIM(cst_lastname),
                CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                     ELSE 'Unknown'
                END,
                CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                     ELSE 'Unknown'
                END,
                cst_create_date
            FROM (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                FROM bronze.crm_cust_info
            )
        """).collect()

        # 2. Truncate silver.crm_prd_info
        session.sql("TRUNCATE TABLE silver.crm_prd_info").collect()

        # 2.1 Insert into silver.crm_prd_info
        session.sql("""
            INSERT INTO silver.crm_prd_info (
                prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
            )
            SELECT
                prd_id, REPLACE(SUBSTR(prd_key, 1, 5), '-', '_'), 
                SUBSTR(prd_key, 7, LENGTH(prd_key)), prd_nm, 
                IFNULL(prd_cost, 0),
                CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'Unknown'
                END,
                prd_start_dt,
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
            FROM bronze.crm_prd_info
        """).collect()

        # 3. Truncate silver.crm_sales_details
        session.sql("TRUNCATE TABLE silver.crm_sales_details").collect()

        # 3.1 Insert into silver.crm_sales_details
        session.sql("""
            INSERT INTO silver.crm_sales_details (
                sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, 
                sls_due_dt, sls_sales, sls_quantity, sls_price
            )
            SELECT
                sls_ord_num, sls_prd_key, sls_cust_id,
                CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
                     ELSE CAST(CAST(sls_order_dt AS STRING) AS DATE)
                END,
                CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
                     ELSE CAST(CAST(sls_ship_dt AS STRING) AS DATE)
                END,
                CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
                     ELSE CAST(CAST(sls_due_dt AS STRING) AS DATE)
                END,
                CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                     THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                END,
                sls_quantity,
                CASE WHEN sls_price IS NULL OR sls_price <= 0
                     THEN sls_sales / NULLIF(sls_quantity, 0)
                    ELSE sls_price
                END
            FROM bronze.crm_sales_details
        """).collect()

        # 4. Truncate silver.erp_cust_az12
        session.sql("TRUNCATE TABLE silver.erp_cust_az12").collect()

        # 4.1 Insert into silver.erp_cust_az12
        session.sql("""
            INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
            SELECT
                CASE WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4, LENGTH(cid)) ELSE cid END,
                CASE WHEN bdate > CURRENT_DATE() THEN NULL ELSE bdate END,
                CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                     ELSE 'Unknown'
                END
            FROM bronze.erp_cust_az12
        """).collect()

        # 5. Truncate silver.erp_loc_a101
        session.sql("TRUNCATE TABLE silver.erp_loc_a101").collect()

        # 5.1 Insert into silver.erp_loc_a101
        session.sql("""
            INSERT INTO silver.erp_loc_a101 (cid, cntry)
            SELECT REPLACE(cid, '-', ''), 
                   CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
                        ELSE TRIM(cntry)
                   END
            FROM bronze.erp_loc_a101
        """).collect()

        # 6. Truncate silver.erp_px_cat_g1v2
        session.sql("TRUNCATE TABLE silver.erp_px_cat_g1v2").collect()

        # 6.1 Insert into silver.erp_px_cat_g1v2
        session.sql("""
            INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
            SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2
        """).collect()

        return "Data load completed successfully!"

    except Exception as e:
        return f"Error occurred: {str(e)}"
$$;

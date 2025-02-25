/*
=============================================================
Truncate Tables and Load the data into tables
=============================================================
Script purpose: 
  This script creates a procedure named 'load_bronze()' 
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_load'
AS
$$
def run_load(session):
    try:
        # Truncate tables
        session.sql("TRUNCATE TABLE bronze.crm_cust_info").collect()
        session.sql("TRUNCATE TABLE bronze.crm_prd_info").collect()
        session.sql("TRUNCATE TABLE bronze.crm_sales_details").collect()
        session.sql("TRUNCATE TABLE bronze.erp_cust_az12").collect()
        session.sql("TRUNCATE TABLE bronze.erp_loc_a101").collect()
        session.sql("TRUNCATE TABLE bronze.erp_px_cat_g1v2").collect()

        # COPY INTO commands
        session.sql("COPY INTO bronze.crm_cust_info FROM @crm_stage/cust_info.csv FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='\"', FIELD_DELIMITER = ',')").collect()
        
        session.sql("COPY INTO bronze.crm_prd_info FROM @crm_stage/prd_info.csv FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='\"', FIELD_DELIMITER = ',')").collect()
        
        session.sql("COPY INTO bronze.crm_sales_details FROM @crm_stage/sales_details.csv FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='\"', FIELD_DELIMITER = ',')").collect()
        
        session.sql("COPY INTO bronze.erp_cust_az12 FROM @erp_stage/CUST_AZ12.csv FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='\"', FIELD_DELIMITER = ',')").collect()
        
        session.sql("COPY INTO bronze.erp_loc_a101 FROM @erp_stage/LOC_A101.csv FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='\"', FIELD_DELIMITER = ',')").collect()
        
        session.sql("COPY INTO bronze.erp_px_cat_g1v2 FROM @erp_stage/PX_CAT_G1V2.csv FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY='\"', FIELD_DELIMITER = ',')").collect()

        return "Data successfully loaded into Bronze tables!"

    except Exception as e:
        return f"Error: {str(e)}"
$$;
